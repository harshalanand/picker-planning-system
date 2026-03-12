"""Allocation engine — pure Python, no Streamlit dependency."""
import math, pandas as pd, numpy as np

def t2m(h, m): return float(h * 60 + m)
def m2t(m):
    t = int(round(m)); return f"{t//60:02d}:{t%60:02d}"

def auto_group(bgt):
    bgt = int(bgt)
    if bgt >= 3000: return 'G1', 1
    if bgt >= 2000: return 'G2', 2
    return 'G3', 3

def analyse_demand(do_df, cfg):
    BGT = cfg['bgt']; FILL = cfg['fill_pct']
    floors = sorted(do_df['FLOOR'].unique())
    fqty = do_df.groupby('FLOOR')['DO_QTY'].sum()
    fdos = do_df.groupby('FLOOR')['DO_NO'].count()
    rows = []
    for f in floors:
        qty = int(fqty[f])
        req = max(1, math.ceil(qty / (BGT * FILL / 100)))
        exp = round(qty / (req * BGT) * 100, 1)
        pbd = (do_df[do_df['FLOOR']==f].groupby('PRIORITY')
               .agg(D=('DO_NO','count'), Q=('DO_QTY','sum'))
               .reset_index().sort_values('PRIORITY'))
        rows.append({'FLOOR':int(f),'QTY':int(qty),'DOS':int(fdos[f]),
                     'REQ':int(req),'EXP':float(exp),
                     'PRIO':[{'PRIORITY':int(r['PRIORITY']),'D':int(r['D']),'Q':int(r['Q'])}
                              for _,r in pbd.iterrows()]})
    return {'floors':rows,'total_qty':int(do_df['DO_QTY'].sum()),
            'total_dos':len(do_df),'total_req':int(sum(r['REQ'] for r in rows))}

# ─────────────────────────────────────────────────────────────────────────────
#  ALLOCATION ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def allocate_plan(do_df, machine_df, cfg, demand, prev_state=None, skip_dos=None):
    """
    prev_state : {(machine_no, floor): {cap_used, avail_min}} — loaded from DB for multi-plan day
    skip_dos   : set of do_no strings already allocated today
    G1 → high priority  |  G2 → mid  |  G3 → last priority
    Continuous greedy, no idle gaps.
    """
    S = cfg['start_min']; LS = cfg['lunch_s']; LE = cfg['lunch_e']
    BGT_DEF = cfg['bgt']; EFF = cfg['eff_min']; WH_END = cfg['wh_end']
    if prev_state is None: prev_state = {}
    if skip_dos is None: skip_dos = set()

    # Normalise machines + auto-group
    mdf = machine_df.copy()
    mdf.columns = [c.strip().upper().replace(' ','_').replace('/','_') for c in mdf.columns]
    sc = next((c for c in mdf.columns if 'SCAN' in c), None)
    if sc and sc != 'SCANNER_NAME': mdf.rename(columns={sc:'SCANNER_NAME'}, inplace=True)
    mdf = mdf.drop_duplicates(subset=['MACHINE_NO'], keep='first').reset_index(drop=True)
    bgt_col = 'BGT_PICKER' if 'BGT_PICKER' in mdf.columns else None
    mdf['BGT_MACHINE'] = mdf[bgt_col].fillna(BGT_DEF).astype(int) if bgt_col else BGT_DEF
    mdf['GROUP'], mdf['GROUP_TIER'] = zip(*mdf['BGT_MACHINE'].apply(auto_group))
    mdf = mdf.sort_values(['GROUP_TIER','BGT_MACHINE'], ascending=[True,False]).reset_index(drop=True)
    all_m = mdf.to_dict('records')

    # Priority tier cuts: top⅓→G1, middle⅓→G2, last⅓→G3
    all_prios = sorted(do_df['PRIORITY'].unique()); n_p = len(all_prios)
    cut1 = all_prios[max(0, n_p//3 - 1)]; cut2 = all_prios[max(0, 2*n_p//3 - 1)]
    def ptier(p): return 1 if p<=cut1 else (2 if p<=cut2 else 3)

    # Floor → machine pool assignment
    pool_map = {}; cur = 0
    for row in sorted(demand['floors'], key=lambda r: -r['REQ']):
        f = row['FLOOR']; req = row['REQ']
        cnt = min(req, len(all_m)-cur)
        pool_map[f] = all_m[cur:cur+cnt]; cur += cnt

    plans = []; skipped = []
    work = do_df.sort_values(['FLOOR','PRIORITY','SEC','DO_NO']).reset_index(drop=True)

    for floor in sorted(do_df['FLOOR'].unique()):
        pool = pool_map.get(floor, [])
        if not pool:
            for _, dr in work[work['FLOOR']==floor].iterrows():
                if str(dr['DO_NO']) not in skip_dos:
                    skipped.append((str(dr['DO_NO']), f'No machines for Floor {floor}'))
            continue

        # Build picker state — restore from prev_state for multi-plan
        pk = []
        for m in pool:
            key = (str(m['MACHINE_NO']), floor)
            prev = prev_state.get(key, {})
            cap = int(prev.get('cap_used', 0))
            avail = float(prev.get('avail_min', S))
            bgt = int(m['BGT_MACHINE'])
            if bgt - cap <= 0: continue          # fully used today
            if avail >= WH_END: continue         # no time left today
            pk.append({'cap':cap, 'avail':avail, 'bgt':bgt,
                       'grp':m['GROUP'], 'tier':m['GROUP_TIER'], 'machine':m})

        if not pk:
            for _, dr in work[work['FLOOR']==floor].iterrows():
                if str(dr['DO_NO']) not in skip_dos:
                    skipped.append((str(dr['DO_NO']), f'F{floor}: all pickers exhausted for today'))
            continue

        for _, dr in work[work['FLOOR']==floor].sort_values(['PRIORITY','SEC','DO_NO']).iterrows():
            do_no = str(dr['DO_NO'])
            if do_no in skip_dos: continue
            prio = int(dr['PRIORITY']); qty = int(dr['DO_QTY']); tier = ptier(prio)

            assigned = False
            for try_tier in [tier, tier-1, tier+1, 1, 2, 3]:
                if try_tier < 1 or try_tier > 3: continue
                cands = [(i,p) for i,p in enumerate(pk)
                         if p['tier']==try_tier and p['bgt']-p['cap']>=qty]
                if not cands: continue
                cands.sort(key=lambda x: x[1]['avail'])
                bi, chosen = cands[0]
                ipm = chosen['bgt'] / EFF; dur = qty / ipm
                ts = chosen['avail']
                if LS <= ts < LE: ts = LE
                te = adv(ts, dur, LS, LE)
                chosen['cap'] += qty; chosen['avail'] = te
                plans.append({
                    'priority':prio, 'do_no':do_no,
                    'sto_no':str(dr.get('STO_NO','')), 'st_cd':str(dr.get('ST_CD','')),
                    'st_nm':str(dr.get('ST_NM','')), 'floor':int(floor), 'sec':str(dr['SEC']),
                    'do_qty':qty, 'picker_no':bi+1, 'machine_no':str(chosen['machine']['MACHINE_NO']),
                    'scanner_name':str(chosen['machine'].get('SCANNER_NAME','')),
                    'grp':chosen['grp'], 'bgt_machine':int(chosen['bgt']),
                    'start_time':m2t(ts), 'end_time':m2t(te),
                    'duration_min':round(dur,2), 'pcs_per_min':round(ipm,4),
                    'cap_used':int(chosen['cap']), 'util_pct':round(chosen['cap']/chosen['bgt']*100,1),
                    'remaining':int(chosen['bgt']-chosen['cap']),
                    'over_wh':int(te>WH_END), '_avail_min':float(te),
                })
                assigned = True; break
            if not assigned:
                skipped.append((do_no, f'F{floor} P{prio}: no picker with capacity+time'))
