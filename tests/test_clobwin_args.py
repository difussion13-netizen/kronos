#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_clobwin_args — аргументы, которые generator датасета передаёт ЧЕРЕЗ пул.

Класс бага, из-за которого 20.09 упал passC: `run_day` начал читать `a.depth_cents`,
а кортеж `work = [(...)]` и `Namespace(...)` внутри `_run_worker` никто не обновил.
Атрибут есть у argparse-парсера в `main()`, но НЕ в объекте, который живёт в воркере
процесса, — поэтому ни один прогон до passC не жаловался, а мои оффлайн-проверки
строили Namespace руками и проходили. Отсюда две проверки: статическая (какие `a.X`
читают функции пула против ключей Namespace воркера и против длины кортежа) и
функциональная (вызвать `_run_worker` ровно так, как его вызывает пул).
"""
import argparse
import ast
import gzip
import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "analysis"))
import clobwin as C                                             # noqa: E402

SRC = os.path.join(HERE, "..", "analysis", "clobwin.py")
POOL_FUNCS = ("run_day", "do_minutes")       # функции, куда пул несёт ns, а не a=парсер


def _func(tree, name):
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return n
    raise AssertionError(f"функция {name} не найдена — проверка нечего не знает и бессмысленна")


def test_namespace_keys():
    tree = ast.parse(open(SRC, encoding="utf-8").read())
    keys = set()
    for n in ast.walk(_func(tree, "_run_worker")):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "Namespace"):
            keys |= {k.arg for k in n.keywords if k.arg}
    assert keys, "_run_worker не строит Namespace — проверка больше ничего не знает"
    for fname in POOL_FUNCS:
        fn = _func(tree, fname)
        arg = fn.args.args[0].arg                      # обычно 'a'
        used = {n.attr for n in ast.walk(fn)
                if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)
                and n.value.id == arg}
        missing = used - keys
        assert not missing, (f"{fname} читает a.{sorted(missing)}, а в Namespace воркера "
                             f"есть только {sorted(keys)} — в пуле это AttributeError "
                             f"после 8.8ГБ стрима")


def test_tuple_arity():
    tree = ast.parse(open(SRC, encoding="utf-8").read())
    main = _func(tree, "main")
    n_in = None
    for n in ast.walk(main):
        if (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "work"
                                              for t in n.targets)
                and isinstance(n.value, ast.ListComp)
                and isinstance(n.value.elt, ast.Tuple)):
            n_in = len(n.value.elt.elts)
    assert n_in, "не найден work = [(...)] — проверь, не переехал ли он"
    worker = _func(tree, "_run_worker")
    n_out = None
    for n in ast.walk(worker):
        if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Tuple):
            n_out = len(n.targets[0].elts)
            break
    assert n_out, "_run_worker не распаковывает кортеж"
    assert n_in == n_out, (f"в кортеже work {n_in} элементов, а _run_worker распаковывает "
                           f"{n_out}: пул упадёт на первом же сутке")


def test_worker_end_to_end():
    """один вызов _run_worker — так, как его зовёт pool.imap_unordered."""
    tree = ast.parse(open(SRC, encoding="utf-8").read())
    worker = _func(tree, "_run_worker")
    n_args = len(next(n for n in ast.walk(worker)
                      if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Tuple)
                      ).targets[0].elts)
    start = 1_788_825_700
    end = start + 300

    def book(tok, bids, asks, ts):
        # timestamp = ts, а не end: чекпоинт c открывается, когда ОСТАТОК до конца
        # — строго меньше c, и вся соль теста в правильном чередовании состояний.
        return ('{"t":%d,"raw":{"event_type":"book","asset_id":"%s","market":"0xm",'
                '"bids":[%s],"asks":[%s],"timestamp":"%d"}}' % (
                    ts * 10 ** 9, tok,
                    ",".join('{"price":"%s","size":"%s"}' % x for x in bids),
                    ",".join('{"price":"%s","size":"%s"}' % x for x in asks), ts))

    lines = [book("111", [("0.50", "120")], [("0.60", "90")], end - 200).encode(),
             book("111", [("0.54", "30")], [("0.56", "250")], end - 5).encode()]
    with tempfile.TemporaryDirectory() as tmp:
        real_df, real_il = C.day_files, C.iter_lines
        C.day_files = lambda *a, **k: ["clob_x.jsonl.gz"]
        C.iter_lines = lambda *a, **k: list(lines)
        try:
            tm = {"111": {"start": start, "end": end, "asset": "btc", "code": "5m",
                          "up": True, "slug": "s"}}
            args = ("bucket", "prefix", "20260918", tmp, tm, False, {"btc"})
            args = args + (2.0,) if n_args == len(args) + 1 else args
            assert len(args) == n_args, (
                f"тест рассуждает про {n_args} аргументов, а я собрал {len(args)}: "
                "либо воркер сменил сигнатуру, либо тест пора переписать")
            day = C._run_worker(args)
            rows = list(csv_reader(os.path.join(tmp, "windows_20260918.csv")))
            h, r = rows[0], rows[1]
            at = lambda c: r[h.index(c)]
            # Чекпоинт c замораживает состояние ДО того сообщения, при котором остаток
            # до конца окна стал < c. Первая книга (end-200) открыла только cp240 — там
            # состояния ещё нет (пусто). Вторая (end-5) открыла 120/60/30/15 -> в них
            # ПЕРВАЯ книга; cp5 и cp0 хвостом дозаполнены ВТОРОЙ.
            assert at("b240") == "", r
            for c in (120, 60, 30, 15):
                assert (at(f"b{c}"), at(f"a{c}")) == ("0.5", "0.6"), (c, r)
            for c in (5, 0):
                assert (at(f"b{c}"), at(f"a{c}")) == ("0.54", "0.56"), (c, r)
            # d2* считается только потому, что depth_cents доехал до run_day: при 0¢
            # глубина вырождается в топа (30 против 120 у первой книги).
            assert at("d2b0") == "30.0", r
            print(f"worker end-to-end OK ({day}, {n_args} арг.)")
        finally:
            C.day_files, C.iter_lines = real_df, real_il


def csv_reader(path):
    import csv
    with open(path) as f:
        return list(csv.reader(f))


if __name__ == "__main__":
    test_namespace_keys()
    test_tuple_arity()
    test_worker_end_to_end()
    print("test_clobwin_args: OK")
