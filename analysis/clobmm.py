#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""clobmm.py — MM-фаза: бумажный симулятор двустороннего котинга со скью якоря.

Гейты и модель зафиксированы ЗАРАНЕЕ в docs/plan-mm.md. Данные: windows_*.csv и
minutes_*.csv из outdir clobwin (s3://…/kronos/win) — новых тяжёлых прогонов не
нужно. P&L — ВЕРХНЯЯ граница (очередей/глубины нет), пессимизм полем f: цена
должна пересечь наш уровень на >f. Информация якоря строго до решения (урок E3):
минутные close ≤ t−60с, σ-палки ≤ t−300с. Резолв-ярлык = прокси fin≥bench по
нашим минуткам (тай → Up), как в clobzero/E2. Торгового кода нет.

    python3 clobmm.py --win /tmp/win [--ds /tmp/ds] [--no-sweep]

Гейты (первичная конфигурация k=2.5, f=0.5¢, anchor on):
  M1 ≥70% дней P&L>0 и медиана дневного P&L>0;  M2 медиана markout синглов > −0.5¢;
  M3 P&L(anchor)−P&L(neutral) > 0 на ≥2/3 дней;  M4 P&L>0 в ≥2/3 сетки k и ≥2/3 сетки f.
"""
import argparse
import bisect
import csv
import glob
import json
import math
import os

SQ2PI = math.sqrt(2.0 * math.pi)
REB = 0.2 * 0.0625


def phi(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def pdf(x):
    return math.exp(-0.5 * x * x) / SQ2PI


def reb(p):
    return REB * min(max(p, 0.0), 1.0) * (1.0 - min(max(p, 0.0), 1.0)) * 100.0


def cp_list(code):
    if code == "5m":
        return [240, 120, 60, 30, 15, 5, 0]
    if code == "15m":
        return [600, 240, 120, 60, 30, 15, 5, 0]
    return [1800, 600, 240, 120, 60, 30, 15, 5, 0]


def load_windows(win_dir, assets, codes):
    rows = []
    for path in sorted(glob.glob(os.path.join(win_dir, "windows_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["asset"] in assets and r["code"] in codes:
                    rows.append(r)
    rows.sort(key=lambda r: r["start"])
    return rows


def load_venue(path, code=None):
    """{(asset,start) -> {"ptb","fp"}} из кэша markcalc (--venue-cache /tmp/venue.jsonl).

    Нужен затем, чтобы ярлик резолва в симе был ТЕМ ЖЕ, чем меряется живая стратегия
    (гейт №2 считается на деньгах площадки: finalPrice >= priceToBeat). Клиновый
    прокси fin>=bench оптимистичнее примерно вчетверо (блок G в markcalc: −448.74¢
    прокси против −1648.74¢ на платеже по одним и тем же 146 синглам), и прогон с
    --venue-cache обязан быть несравним с прогоном без него — поэтому покрытие
    печатается отдельной строкой, а молча подмешивать один ярлык в другой нельзя.
    """
    out = {}
    if not path or not os.path.exists(path):
        return out
    with open(path) as f:
        for ln in f:
            try:
                r = json.loads(ln)
            except ValueError:
                continue
            if code and r.get("code") not in (None, code):
                continue
            if r.get("ptb") is None or r.get("fp") is None:
                continue
            out[(r["asset"], int(r["start"]))] = {"ptb": float(r["ptb"]),
                                                   "fp": float(r["fp"])}
    return out


def load_minutes(win_dir):
    per = {}
    for path in sorted(glob.glob(os.path.join(win_dir, "minutes_*.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                d = per.setdefault(r["asset"], {"m": [], "c": []})
                d["m"].append(int(r["minute"])); d["c"].append(float(r["close"]))
    for d in per.values():
        o = sorted(range(len(d["m"])), key=lambda i: d["m"][i])
        d["m"] = [d["m"][i] for i in o]; d["c"] = [d["c"][i] for i in o]
        n = len(d["m"])
        cs = [0.0] * n; c2 = [0.0] * n; vc = [0] * n   # префиксы ПО ПОЗИЦИЯМ m,
        for i in range(1, n):                            # дырявые минуты не сдвигают
            cs[i] = cs[i - 1]; c2[i] = c2[i - 1]; vc[i] = vc[i - 1]
            if d["m"][i] - d["m"][i - 1] <= 120 and d["c"][i - 1] > 0:
                r = math.log(d["c"][i] / d["c"][i - 1]) * 1e4
                cs[i] += r; c2[i] += r * r; vc[i] += 1
        d["cs"], d["c2"], d["vc"] = cs, c2, vc
    return per


def mget(per, asset, ts):
    d = per.get(asset)
    if not d:
        return None
    i = bisect.bisect_right(d["m"], ts) - 1
    return d["c"][i] if i >= 0 else None


def load_bars(ds_dir, assets):
    out = {}
    if not ds_dir or not os.path.isdir(ds_dir):
        return out
    for a in assets:
        p = os.path.join(ds_dir, f"{a}_300s.csv")
        if os.path.exists(p):
            ts, vv = [], []
            with open(p) as f:
                for r in csv.DictReader(f):
                    ts.append(int(float(r["t0"])))
                    vv.append(max(0.5, float(r.get("volat12") or 8.0)))
            out[a] = (ts, vv)
    return out


class Sigma:
    """σ б.п./5м-бар строго до t: volat12 датасета (t0+300 ≤ t) или std 120 мин."""

    def __init__(self, per, bars):
        self.per, self.bars, self.memo = per, bars, {}

    def _from_minutes(self, asset, t):
        d = self.per.get(asset)
        if not d or len(d["m"]) < 40:
            return 0.0
        j = bisect.bisect_right(d["m"], t) - 1
        lo = max(0, j - 120)
        n = d["vc"][j] - d["vc"][lo]
        if n < 30:
            return 0.0
        s = d["cs"][j] - d["cs"][lo]; s2 = d["c2"][j] - d["c2"][lo]
        return math.sqrt(max(0.0, s2 / n - (s / n) ** 2)) * math.sqrt(5.0)

    def get(self, asset, t):
        key = (asset, t // 60)
        v = self.memo.get(key)
        if v is not None:
            return v
        v = 0.0
        bd = self.bars.get(asset)
        if bd:
            i = bisect.bisect_right(bd[0], t - 301) - 1
            if i >= 0:
                v = bd[1][i]
        if v <= 0:
            v = self._from_minutes(asset, t)
        v = max(2.0, v)
        self.memo[key] = v
        return v


def qmed(v, p=0.5):
    v = sorted(v)
    return round(v[int(p * (len(v) - 1))], 2) if v else 0.0


def sim(rows, per, sig, anchor, k, f, venue=None, inv_usd=0.0, max_part=0.0,
        centers=None):
    """Один рынок = одна независимая мини-книга (так устроено разрешение updown).

    Единицы (тут мы уже обжигались, поэтому словами): g = ¢/акц; lots = inv_usd/
    (цена·100) = ЧИСЛО СОТЕН АКЦИЙ; g·lots = доллары. При inv_usd=0 lots=1, т.е.
    «сто акций на касание» — историческая конвенция вывода («P&L …$»), и арифметика
    дословно прежняя: гейты M1–M4 остаются сравнимыми через границу. markout
    СОЗНАТЕЛЬНО не масштабируется: порог −0.5¢ определён на акцию.
    max_part — отказ выставлять заявку, если она больше max_part доли оборота окна:
    единственный доступный из снапшотов тест «а было ли кем торговать».
    """
    tot = {"pnl": 0.0, "fills": 0, "pairs": 0, "singles": 0, "skip": 0,
           "thin": 0, "venue": 0, "proxy": 0}
    marks, marks_x = [], []
    iv = []          # (t_open, t_close, usd) — время, пока в рынке держатся деньги
    days = {}

    def add(x, start):
        tot["pnl"] += x
        d = start // 86400
        days[d] = days.get(d, 0.0) + x

    for r in rows:
        asset = r["asset"]; start = int(r["start"]); end = int(r["end"]); code = r["code"]
        bench = mget(per, asset, start); fin = mget(per, asset, end - 1)
        if bench is None or fin is None or bench <= 0:
            tot["skip"] += 1
            continue
        y = 1 if fin >= bench else 0
        if venue is not None:
            rec = venue.get((asset, start))
            if not rec:
                tot["skip"] += 1
                tot["skip_no_venue"] = tot.get("skip_no_venue", 0) + 1
                continue
            y = 1 if rec["fp"] >= rec["ptb"] else 0
            tot["venue"] += 1
        else:
            tot["proxy"] += 1
        vol = float(r.get("vol_usd") or 0.0)
        if max_part > 0 and (vol <= 0 or inv_usd / vol > max_part):
            tot["thin"] += 1
            continue
        mids = {}
        for c in cp_list(code):
            b = r.get("b%d" % c); a = r.get("a%d" % c)
            if b not in (None, "") and a not in (None, ""):
                mids[c] = (float(b) + float(a)) / 2.0
        cps = cp_list(code)
        pos, px, lots_cur, t_open = 0, 0.0, 0.0, None
        for c in [x for x in cps if x >= 60]:
            i = cps.index(c)
            c2 = cps[i + 1] if i + 1 < len(cps) else None
            mid2 = mids.get(c2) if c2 is not None else None
            if mid2 is None:
                continue
            t1 = end - c
            spot = mget(per, asset, t1 - 60) or bench
            d_bps = (spot / bench - 1.0) * 1e4
            tau = float(max(10, c))
            s5 = sig.get(asset, t1 - 60)
            x = d_bps / (s5 * math.sqrt(tau / 300.0))
            # Learned center override
            if centers and (asset, start) in centers:
                pa = centers[(asset, start)]
                sigma_p = math.sqrt(max(0.01, pa * (1.0 - pa)))
                step = float(max(15, min(c - c2, int(tau))))
                h = max(0.005, k * sigma_p * math.sqrt(step / tau))
            else:
                pa = min(max(phi(x), 0.01), 0.99)
                step = float(max(15, min(c - c2, int(tau))))
                h = max(0.005, k * pdf(x) * math.sqrt(step / tau))
            center = pa if anchor else mids.get(c, pa)
            bid = min(max(center - h, 0.005), 0.99)
            ask = min(max(bid + 0.01, center + h), 0.995)
            eb = bid - mid2 if (pos <= 0 and mid2 <= bid - f) else -1.0
            ea = mid2 - ask if (pos >= 0 and mid2 >= ask + f) else -1.0
            if eb < 0 and ea < 0:
                continue
            # цена касания -> размер заявки в «сотнях акций»; при inv_usd=0 lots=1
            pl = (1.0 - ask) if ea > eb else bid
            lots = (inv_usd / (pl * 100.0)) if inv_usd > 0 else 1.0
            if ea > eb:                                   # спросили наш ASK (продали YES)
                if pos == 1:                             # выход по YES = пара
                    g = (ask - px) * 100.0
                    add(lots_cur * (g + reb(ask)), start); marks_x.append(g)
                    tot["pairs"] += 1; pos = 0
                    iv.append((t_open, t1, lots_cur * pl * 100.0)); t_open = None
                elif pos == 0:                            # без инвентаря = покупка NO по 1-ask
                    pos, px, lots_cur = -1, 1.0 - ask, lots
                    tot["fills"] += 1
                    add(reb(1.0 - ask) * lots, start); t_open = t1
            else:                                         # наш BID забрали (купили YES)
                if pos == -1:                             # YES+NO = $1, закрытие пары
                    g = (1.0 - bid - px) * 100.0
                    add(lots_cur * (g + reb(bid)), start); marks_x.append(g)
                    tot["pairs"] += 1; pos = 0
                    iv.append((t_open, t1, lots_cur * pl * 100.0)); t_open = None
                elif pos == 0:
                    pos, px, lots_cur = 1, bid, lots
                    tot["fills"] += 1
                    add(reb(bid) * lots, start); t_open = t1
        if pos != 0:
            val = y if pos == 1 else 1 - y
            g = (val - px) * 100.0 + reb(px)
            add(g * lots_cur, start); marks.append(g); tot["singles"] += 1
            iv.append((t_open if t_open is not None else start, end,
                       lots_cur * px * 100.0))
    dv = list(days.values())
    return dict(pnl=round(tot["pnl"], 1), fills=tot["fills"], pairs=tot["pairs"],
                thin=tot["thin"], venue_n=tot["venue"], proxy_n=tot["proxy"],
                no_venue=tot.get("skip_no_venue", 0), iv=iv,
                singles=tot["singles"], skip=tot["skip"],
                mark_s_med=qmed(marks), mark_s_p90=qmed(marks, .9),
                mark_x_med=qmed(marks_x),
                days_n=len(dv), days_pos=sum(1 for x in dv if x > 0),
                day_med=qmed(dv), days=days)


def fmt(tag, m):
    return (f"{tag:<26} fills {m['fills']:>5}  pairs {m['pairs']:>4}  singl {m['singles']:>5}"
            f"  P&L {m['pnl']:>12.1f}$  дн+ {m['days_pos']:>2}/{m['days_n']:<2}"
            f"  markout_med {m['mark_s_med']:>6.2f}¢  p90 {m['mark_s_p90']:>5.1f}¢")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--win", default="/tmp/win")
    ap.add_argument("--ds", default="/tmp/ds", help="датасеты dataset.py (для volat12); нет — σ из минуток")
    ap.add_argument("--assets", default="btc,eth,sol,xrp")
    ap.add_argument("--codes", default="5m,15m")
    ap.add_argument("--k", type=float, default=2.5, help="множитель полуширины")
    ap.add_argument("--fill-margin", type=float, default=0.5, help="¢ пессимизма касания")
    ap.add_argument("--no-sweep", action="store_true")
    ap.add_argument("--venue-cache", default="",
                    help="ярлык резолва = платёж площадки (ptb/fp из кэша markcalc); "
                         "окна без платежа ПРОПУСКАЮТСЯ, а не размечаются прокси")
    ap.add_argument("--inventory-usd", type=float, default=0.0,
                    help="размер заявки в долларах (0 = 1 акция = историческая "
                         "конвенция P&L-в-центах-на-акцию, не трогает гейты)")
    ap.add_argument("--max-part", type=float, default=0.0,
                    help="не выставлять, если заявка > этой доли оборота окна "
                         "(0.05 = 5%%; 0 = фильтр выключен)")
    ap.add_argument("--centers-file", default="",
                    help="JSON {(asset,start): center} — замена Gaussian Φ "
                         "(из price_model.py --export-centers)")
    a = ap.parse_args()
    assets = set(x.strip() for x in a.assets.split(","))
    codes = set(x.strip() for x in a.codes.split(","))
    rows = load_windows(a.win, assets, codes)
    print(f"# clobmm: окон {len(rows)} (активы {sorted(assets)}, коды {sorted(codes)}); "
          f"k={a.k}, f={a.fill_margin}¢")
    if not rows:
        raise SystemExit("нет windows_*.csv — см. docs/plan-mm.md «Запуск»")
    per = load_minutes(a.win)
    sig = Sigma(per, load_bars(a.ds, assets))
    print(f"  источник σ: {'dataset volat12' if sig.bars else 'std 120 мин (fallback)'}")
    f0 = a.fill_margin / 100.0
    venue = load_venue(a.venue_cache, "5m") if a.venue_cache else None
    if venue is not None:
        print(f"  ярлык = платёж площадки: платежей на {len(venue)} окон; при этом "
              f"ярлыке P&L и все четыре гейта НЕ сравнимы с прогоном по клиновому "
              f"прокси — это другой стандарт, и он честнее (см. загрузчик)")
    iv_kw = dict(venue=venue, inv_usd=a.inventory_usd, max_part=a.max_part)
    # Загрузка learned centers (если есть)
    centers = None
    if a.centers_file and os.path.exists(a.centers_file):
        with open(a.centers_file) as f:
            raw = json.load(f)
        centers = {(k.split("|")[0], int(k.split("|")[1])): v
                   for k, v in raw.items()}
        print(f"  learned centers: {len(centers)} окон из {a.centers_file}")
    on = sim(rows, per, sig, True, a.k, f0, centers=centers, **iv_kw)
    neu = sim(rows, per, sig, False, a.k, f0, **iv_kw)
    print()
    print(fmt("primary (anchor on)", on))
    print(fmt("контроль (нейтр. центр)", neu))
    dd = [on["days"].get(d, 0.0) - neu["days"].get(d, 0.0) for d in sorted(on["days"])]
    wins = sum(1 for x in dd if x > 0)
    print(f"  разница по дням (on−нейтр): {wins}/{len(dd)} дней в плюс; "
          f"медиана {qmed(dd)}$")
    m1 = on["days_n"] and on["days_pos"] >= 0.7 * on["days_n"] and on["day_med"] > 0
    m2 = on["mark_s_med"] > -0.5
    m3 = wins >= max(1, -(-2 * len(dd) // 3))
    print("\n== сетка робастности (P&L, $; первичная жирным не выделена — см. выше) ==")
    ks = [1.5, a.k, 4.0]; fs = [0.0, f0, 0.01]
    grid = {}
    for kk in ks:
        for ff in fs:
            r = sim(rows, per, sig, True, kk, ff, centers=centers, **iv_kw)
            grid[(kk, ff)] = r["pnl"]
            print(f"  k={kk:>4} f={100*ff:>5.1f}¢: P&L {r['pnl']:>12.1f}$  "
                  f"days+ {r['days_pos']:>2}/{r['days_n']:<2}  markout {r['mark_s_med']:>6.2f}¢")
    ok_k = sum(1 for kk in ks if grid.get((kk, f0), -1.0) > 0)
    ok_f = sum(1 for ff in fs if grid.get((a.k, ff), -1) > 0)
    m4 = ok_k >= 2 and ok_f >= 2
    res = [("M1 дни+ и медиана P&L > 0", m1),
           ("M2 markout синглов > -0.5¢", m2),
           ("M3 якорь > нейтр. контроля 2/3 дней", m3),
           ("M4 робастность сетки k и f", m4)]
    if a.inventory_usd > 0 or a.max_part > 0 or venue is not None:
        print("\n== ЁМКОСТЬ и исполнимость (верхняя граница: очереди нет) ==")
        if a.max_part > 0:
            print(f"  заявок отброшено по «тонкому окну» (доля > {100*a.max_part:.0f}% "
                  f"оборота): {on['thin']} из {len(rows)}")
        else:
            print(f"  фильтр участия ВЫКЛЮЧЕН (--max-part 0): ни одна заявка не "
                  f"отсекалась по обороту окна; сравнивать этот прогон можно только "
                  f"с самим собой")
        if venue is not None:
            print(f"  ярлык: платёж площадки на {on['venue_n']} окнах, "
                  f"без платежа пропущено {on['no_venue']}")
        sk_min = on.get("skip", 0) - on.get("no_venue", 0)
        if sk_min:
            # без этой строки два прогона (с фильтром участия и без) не сводятся
            # арифметически: len(rows) минус отброшенные «по обороту» ≠ измеренные окна.
            # mget() при дырке в минутах НЕ возвращает None (берёт последнюю доступную),
            # так что «не дошли» почти всегда = актива нет в minutes_*.csv целиком
            bys = {}
            for r in rows:
                t0 = int(r["start"])
                if (mget(per, r["asset"], t0) is None
                        or mget(per, r["asset"], int(r["end"]) - 1) is None):
                    bys[r["asset"]] = bys.get(r["asset"], 0) + 1
            det = ", ".join(f"{k}:{v}" for k, v in sorted(bys.items())) or "—"
            print(f"  до измерения не дошли ещё и так: {sk_min} из {len(rows)} — нет "
                  f"спот-минутки на границе (актив вне покрытия минут или окно до его "
                  f"начала; это пропуск, не ноль в P&L); по активам: {det}")
        iv = sorted(on.get("iv") or [])
        if iv:
            ev = []
            for t0, t1, u in iv:
                ev.append((t0, u)); ev.append((t1, -u))
            ev.sort(); cur = 0.0; pk = []
            for _t, du in ev:
                cur += du
                pk.append(cur)
            pk.sort()
            n = len(pk)
            print(f"  одновременный капитал при ставке ${a.inventory_usd:.0f}/окно: "
                  f"медиана {qmed(pk):.0f}$, p90 {pk[int(0.9*n)]:.0f}$, "
                  f"пик {pk[-1]:.0f}$ на {len(iv)} держаний "
                  f"(0$ = в этот момент ни в одном окне нет позиции)")
            print("  единицы: lots = сотни акций, P&L — доллары; markout — ¢/акц и "
                  "размером заявки НЕ масштабируется (порог гейта M2 именно на акцию). "
                  "Это НЕ «сколько мы бы заработали»: полная исполняемость каждого "
                  "касания assumed. "
                  "Реальная исполняемость = очередь + объём агрессора, их в снапшотах "
                  "нет: следующий шаг — проход по пересечениям между t1/t2 (M6), "
                  "а не игра с размером.")
        else:
            print("  держаний нет: при этих параметрах позиция не открылась ни разу "
                  "(для инвентарного вывода это результат, а не пустота)")
    print("\n== вердикты гейтов MM-фазы ==")
    for name, ok in res:
        print(f"  {'ДА ' if ok else 'НЕТ'}  {name}")
    ny = sum(1 for _, ok in res if not ok)
    print("ИТОГ:", "фаза ММ подтверждена — следующий артефакт: модель очереди "
          "+ полные сутки (см. план)" if ny == 0 else
          f"{ny} «нет» — " + ("направление закрываем с протоколом (2+)" if ny >= 2
          else "граничный результат: расширить период данных и повторить (1)"))
    cut = lambda m: {kk: vv for kk, vv in m.items() if kk not in ("days", "iv")}
    out = dict(primary=cut(on), neutral=cut(neu), diff_days=f"{wins}/{len(dd)}",
               grid={f"k{kk}_f{round(100*ff,1)}c": round(grid[(kk, ff)], 1)
                     for kk in ks for ff in fs},
               gates={n: bool(o) for n, o in res})
    jp = os.path.join(a.win, "mm_summary.json")
    json.dump(out, open(jp, "w"), ensure_ascii=False, indent=1)
    print("сводка ->", jp)


if __name__ == "__main__":
    main()
