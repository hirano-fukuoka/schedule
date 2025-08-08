import os
import sqlite3
from datetime import date, datetime, timedelta
from contextlib import closing

import pandas as pd
import streamlit as st
import plotly.express as px

# ====== UI 設定 ======
st.set_page_config(page_title="工程進捗 見える化", layout="wide")
st.markdown("""
<style>
    body, .stApp { background-color: #FFFFFF !important; }
    .late { color:#fff; background:#d9534f; padding:2px 6px; border-radius:4px; }
    .warn { color:#212529; background:#f0ad4e; padding:2px 6px; border-radius:4px; }
    .ok { color:#fff; background:#5cb85c; padding:2px 6px; border-radius:4px; }
</style>
""", unsafe_allow_html=True)

# ====== 一般化設定（固有名詞を使わない） ======
CONFIG = {
    "FIELD_LABELS": {
        "customer_due": "顧客納期",     # 旧: 納期
        "internal_deadline": "社内締切" # 旧: デッドライン
    },
    "DEFAULT_PROJECT_NAME": "",          # 固有名詞の自動入力はしない
    "DEFAULT_CUSTOMER_NAME": "",
    "DEFAULT_ITEMS": []                  # 固有名詞の部品サンプルは投入しない
}

# ====== 定数 ======
DEFAULT_STAGES = [
    "設計", "外注製作品", "購入部品", "組立", "検査", "試運転", "解体", "出荷"
]

DB_PATH = "progress.db"

# ====== DB 初期化 ======
DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS projects(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    customer TEXT,
    note TEXT
);

CREATE TABLE IF NOT EXISTS items(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    code TEXT,
    description TEXT,
    plan_start DATE,
    plan_finish DATE,
    due DATE,
    hard_deadline DATE,
    UNIQUE(project_id, code)
);

CREATE TABLE IF NOT EXISTS tasks(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    plan_start DATE,
    plan_finish DATE,
    act_start DATE,
    act_finish DATE,
    progress REAL DEFAULT 0.0,
    owner TEXT,
    supplier TEXT,
    memo TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasks_item ON tasks(item_id);
"""

def get_conn():
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

def init_db():
    with closing(get_conn()) as con, con:
        con.executescript(DDL)

init_db()

# ====== ユーティリティ ======
def risk_level(row: pd.Series) -> str:
    today = date.today()
    plan_finish = row.get("plan_finish")
    act_finish = row.get("act_finish")
    progress = float(row.get("progress") or 0.0)

    if progress > 0 and progress < 1 and plan_finish:
        plan_start = row.get("plan_start") or today
        plan_days = max((pd.to_datetime(plan_finish) - pd.to_datetime(plan_start)).days, 1)
        est_finish = today + timedelta(days=int(plan_days * (1 - progress)))
    elif progress >= 1:
        est_finish = act_finish or plan_finish
    else:
        est_finish = plan_finish

    due_candidates = [d for d in [row.get("due"), row.get("hard_deadline"), plan_finish] if pd.notna(d)]
    due = min(pd.to_datetime(x) for x in due_candidates) if due_candidates else None

    if not due or not est_finish:
        return "warn"

    est_finish = pd.to_datetime(est_finish)
    slack_days = (due - est_finish).days

    if slack_days < 0:
        return "late"
    elif slack_days <= 7:
        return "warn"
    return "ok"

def badge(level: str) -> str:
    return {
        "late": '<span class="late">遅延</span>',
        "warn": '<span class="warn">要注意</span>',
        "ok":   '<span class="ok">順調</span>',
    }.get(level, '<span class="warn">要確認</span>')

def df_from_sql(sql, params=()):
    with closing(get_conn()) as con:
        return pd.read_sql_query(sql, con, params=params, parse_dates=["plan_start","plan_finish","act_start","act_finish","due","hard_deadline"])

def execute(sql, params=()):
    with closing(get_conn()) as con, con:
        con.execute(sql, params)

# ====== サイドバー ======
st.sidebar.header("プロジェクト")
proj_df = df_from_sql("SELECT * FROM projects ORDER BY id DESC")
proj_names = ["(新規作成)"] + proj_df["name"].tolist()
sel = st.sidebar.selectbox("選択", proj_names)

if sel == "(新規作成)":
    with st.sidebar.form("new_proj"):
        name = st.text_input("名称", value=CONFIG["DEFAULT_PROJECT_NAME"])
        customer = st.text_input("客先", value=CONFIG["DEFAULT_CUSTOMER_NAME"])
        note = st.text_area("備考", value="")
        submitted = st.form_submit_button("作成")
        if submitted and name.strip():
            execute("INSERT INTO projects(name, customer, note) VALUES(?,?,?)", (name, customer, note))
            st.rerun()
    project_row = None
else:
    project_row = proj_df.loc[proj_df["name"] == sel].iloc[0]
    st.sidebar.caption(f"客先: {project_row['customer']}")

# ====== 初期データ投入 ======
def seed_items(project_id: int):
    if not CONFIG["DEFAULT_ITEMS"]:
        return
    today = date.today()
    for code, desc in CONFIG["DEFAULT_ITEMS"]:
        execute("""
            INSERT OR IGNORE INTO items(project_id, code, description, plan_start, plan_finish)
            VALUES(?,?,?,?,?)
        """, (project_id, code, desc, today, today + timedelta(days=60)))
        item_id = df_from_sql("SELECT id FROM items WHERE project_id=? AND code=?", (project_id, code)).iloc[0]["id"]
        for i, stage in enumerate(DEFAULT_STAGES):
            execute("""
                INSERT INTO tasks(item_id, stage, plan_start, plan_finish, progress)
                VALUES(?,?,?,?,?)
            """, (item_id, stage, today + timedelta(days=7*i), today + timedelta(days=7*(i+1)), 0.0))

# ====== メイン ======
st.title("工程進捗 見える化")

if project_row is None:
    st.info("左のサイドバーからプロジェクトを作成してください。")
    st.stop()

project_id = int(project_row["id"])

if st.sidebar.button("サンプル工程を投入"):
    seed_items(project_id)
    st.success("サンプル工程を投入しました。")
    st.rerun()

# ====== アイテム一覧（編集・削除対応） ======
st.subheader("① アイテム（部品）一覧 / 期日設定")

items = df_from_sql("SELECT * FROM items WHERE project_id=? ORDER BY id", (project_id,))

with st.expander("新規アイテムを追加", expanded=False):
    with st.form("add_item"):
        col1, col2, col3 = st.columns([2,2,2])
        with col1:
            code = st.text_input("アイテム名")
            desc = st.text_input("説明", value="")
        with col2:
            plan_start = st.date_input("計画開始日", value=date.today())
            plan_finish = st.date_input("計画完了日", value=date.today()+timedelta(days=30))
        with col3:
            due = st.date_input(f'{CONFIG["FIELD_LABELS"]["customer_due"]}（任意）', value=None)
            hard_deadline = st.date_input(f'{CONFIG["FIELD_LABELS"]["internal_deadline"]}（任意）', value=None)
        submitted = st.form_submit_button("追加")
        if submitted and code.strip():
            execute("""
                INSERT OR IGNORE INTO items(project_id, code, description, plan_start, plan_finish, due, hard_deadline)
                VALUES(?,?,?,?,?,?,?)
            """, (project_id, code.strip(), desc, plan_start, plan_finish, due, hard_deadline))
            st.success("アイテムを追加しました。")
            st.rerun()

if items.empty:
    st.warning("アイテムが未登録です。")
else:
    items_view = items.copy()
    items_view["risk"] = items_view.apply(risk_level, axis=1)
    items_view["状態"] = items_view["risk"].map({"late":"遅延","warn":"要注意","ok":"順調"})

    edit_df = items[["id","code","description","plan_start","plan_finish","due","hard_deadline"]].copy()
    edit_df["削除"] = False

    with st.form("edit_items"):
        edited = st.data_editor(
            edit_df,
            column_config={
                "code": st.column_config.TextColumn("アイテム名"),
                "description": st.column_config.TextColumn("説明"),
                "plan_start": st.column_config.DateColumn("計画開始日"),
                "plan_finish": st.column_config.DateColumn("計画完了日"),
                "due": st.column_config.DateColumn(CONFIG["FIELD_LABELS"]["customer_due"]),
                "hard_deadline": st.column_config.DateColumn(CONFIG["FIELD_LABELS"]["internal_deadline"]),
                "削除": st.column_config.CheckboxColumn("削除")
            },
            hide_index=True,
            use_container_width=True,
            num_rows="fixed",
        )
        confirm_delete = st.checkbox("※ 削除を実行する（チェックされた行を本当に削除します）")
        do_save = st.form_submit_button("変更を保存")

        if do_save:
            def _d(v):
                return None if pd.isna(v) else pd.to_datetime(v).date()
            deleted, updated = 0, 0
            with closing(get_conn()) as con, con:
                for _, r in edited.iterrows():
                    iid = int(r["id"])
                    if bool(r["削除"]):
                        if confirm_delete:
                            con.execute("DELETE FROM items WHERE id=?", (iid,))
                            deleted += 1
                        continue
                    con.execute("""
                        UPDATE items
                           SET code=?,
                               description=?,
                               plan_start=?,
                               plan_finish=?,
                               due=?,
                               hard_deadline=?
                         WHERE id=?
                    """, (
                        (r["code"] or "").strip(),
                        r.get("description") or "",
                        _d(r.get("plan_start")),
                        _d(r.get("plan_finish")),
                        _d(r.get("due")),
                        _d(r.get("hard_deadline")),
                        iid
                    ))
                    updated += 1
            if updated: st.success(f"更新 {updated} 件")
            if deleted and confirm_delete: st.success(f"削除 {deleted} 件")
            st.rerun()

    st.markdown("**状態ビュー（読み取り）**")
    display = items_view[["code","plan_start","plan_finish","due","hard_deadline","状態"]].rename(columns={
        "code":"アイテム",
        "due": CONFIG["FIELD_LABELS"]["customer_due"],
        "hard_deadline": CONFIG["FIELD_LABELS"]["internal_deadline"]
    })
    st.write(display.to_html(escape=False, index=False), unsafe_allow_html=True)

# ====== ② タスク（工程）編集 ======
st.subheader("② タスク（工程）編集")

items_for_tasks = df_from_sql("SELECT * FROM items WHERE project_id=? ORDER BY id", (project_id,))
if items_for_tasks.empty:
    st.info("先に『① アイテム一覧』でアイテムを作成してください。")
    st.stop()

sel_item = st.selectbox("対象アイテム", items_for_tasks["code"].tolist())
item_id = int(items_for_tasks.loc[items_for_tasks["code"] == sel_item].iloc[0]["id"])

tasks = df_from_sql("SELECT * FROM tasks WHERE item_id=? ORDER BY id", (item_id,))
with st.form("edit_tasks"):
    st.caption("行を編集して『保存』を押してください。新規行を追加することもできます。削除はチェック＋下部の確認で実行。")
    # 編集用テーブルを準備
    task_df = tasks.copy()
    task_df["削除"] = False

    edited = st.data_editor(
        task_df,
        column_config={
            "stage": st.column_config.SelectboxColumn("ステージ", options=DEFAULT_STAGES),
            "plan_start": st.column_config.DateColumn("計画開始"),
            "plan_finish": st.column_config.DateColumn("計画完了"),
            "act_start": st.column_config.DateColumn("実績開始"),
            "act_finish": st.column_config.DateColumn("実績完了"),
            "progress": st.column_config.NumberColumn("進捗率", min_value=0.0, max_value=1.0, step=0.05),
            "owner": st.column_config.TextColumn("担当"),
            "supplier": st.column_config.TextColumn("外注/購買先"),
            "memo": st.column_config.TextColumn("メモ"),
            "削除": st.column_config.CheckboxColumn("削除"),
            "id": None, "item_id": None  # 非表示
        },
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic"
    )

    col1, col2 = st.columns([1,2])
    with col2:
        confirm_delete_task = st.checkbox("※ タスク削除を実行する（チェックされた行を本当に削除）")
    if st.form_submit_button("保存"):
        def _d(v):
            return None if pd.isna(v) else pd.to_datetime(v).date()
        inserted, updated, deleted, failed = 0, 0, 0, 0
        with closing(get_conn()) as con, con:
            for _, r in edited.iterrows():
                # 既存行か新規行か
                row_id = r.get("id")
                is_new = pd.isna(row_id) or row_id is None

                # 削除チェック
                if not is_new and bool(r.get("削除")):
                    if confirm_delete_task:
                        try:
                            con.execute("DELETE FROM tasks WHERE id=?", (int(row_id),))
                            deleted += 1
                        except Exception:
                            failed += 1
                    continue

                # 空行（ステージ無し/計画無しなど）はスキップ
                stage = (r.get("stage") or "").strip()
                if stage == "":
                    continue

                record = (
                    stage,
                    _d(r.get("plan_start")),
                    _d(r.get("plan_finish")),
                    _d(r.get("act_start")),
                    _d(r.get("act_finish")),
                    float(r.get("progress") or 0.0),
                    (r.get("owner") or "").strip(),
                    (r.get("supplier") or "").strip(),
                    (r.get("memo") or "").strip()
                )

                try:
                    if is_new:
                        con.execute("""
                            INSERT INTO tasks(item_id, stage, plan_start, plan_finish, act_start, act_finish, progress, owner, supplier, memo)
                            VALUES(?,?,?,?,?,?,?,?,?,?)
                        """, (item_id, *record))
                        inserted += 1
                    else:
                        con.execute("""
                            UPDATE tasks
                               SET stage=?,
                                   plan_start=?,
                                   plan_finish=?,
                                   act_start=?,
                                   act_finish=?,
                                   progress=?,
                                   owner=?,
                                   supplier=?,
                                   memo=?
                             WHERE id=?
                        """, (*record, int(row_id)))
                        updated += 1
                except Exception:
                    failed += 1

        msgs = []
        if inserted: msgs.append(f"追加 {inserted} 件")
        if updated: msgs.append(f"更新 {updated} 件")
        if deleted and confirm_delete_task: msgs.append(f"削除 {deleted} 件")
        if msgs: st.success("、".join(msgs))
        if failed: st.warning(f"失敗 {failed} 件（入力内容を確認してください）")
        st.rerun()

# ====== ③ ガントチャート（予定 / 実績） ======
st.subheader("③ ガントチャート（予定 / 実績）")

def make_gantt(df: pd.DataFrame, mode="plan"):
    if df.empty:
        return None
    g = df.copy()
    g["ItemStage"] = sel_item + "｜" + g["stage"].astype(str)
    if mode == "plan":
        g = g.dropna(subset=["plan_start","plan_finish"])
        g["Start"] = pd.to_datetime(g["plan_start"])
        g["Finish"] = pd.to_datetime(g["plan_finish"])
        color = "#5bc0de"  # 水色
        title = "計画"
    else:
        g = g.dropna(subset=["act_start","act_finish"])
        g["Start"] = pd.to_datetime(g["act_start"])
        g["Finish"] = pd.to_datetime(g["act_finish"])
        color = "#5cb85c"  # 緑
        title = "実績"
    if g.empty:
        return None
    fig = px.timeline(g, x_start="Start", x_end="Finish", y="ItemStage", color_discrete_sequence=[color])
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(height=380, margin=dict(l=10,r=10,t=40,b=10), title=f"{title}ガント（{sel_item}）", plot_bgcolor="white", paper_bgcolor="white")
    return fig

colA, colB = st.columns(2)
with colA:
    plan_fig = make_gantt(tasks, "plan")
    if plan_fig:
        st.plotly_chart(plan_fig, use_container_width=True)
    else:
        st.info("計画の期間が未入力のタスクは表示されません。")
with colB:
    act_fig = make_gantt(tasks, "act")
    if act_fig:
        st.plotly_chart(act_fig, use_container_width=True)
    else:
        st.info("実績の開始・完了が未入力のタスクは表示されません。")

# ====== ④ 遅延アラート（アイテム横断） ======
st.subheader("④ 遅延アラート")

alert_rows = []
for _, it in items_for_tasks.iterrows():
    tdf = df_from_sql("SELECT * FROM tasks WHERE item_id=? ORDER BY id", (int(it["id"]),))
    if tdf.empty:
        continue
    # 代表タスクは計画完了が最も遅いもの（全体完了に近い）
    rep = tdf.sort_values("plan_finish").iloc[-1]
    row = {**it.to_dict(),
           "plan_start": rep.get("plan_start"),
           "plan_finish": rep.get("plan_finish"),
           "act_start": rep.get("act_start"),
           "act_finish": rep.get("act_finish"),
           "progress": rep.get("progress")}
    row["risk"] = risk_level(pd.Series(row))
    alert_rows.append(row)

alert = pd.DataFrame(alert_rows)
if alert.empty:
    st.info("アラート対象はありません。")
else:
    alert["badge"] = alert["risk"].map(badge)
    show = alert.sort_values("risk").assign(アイテム=alert["code"])[
        ["badge","アイテム","plan_finish","due","hard_deadline","progress"]
    ].rename(columns={
        "plan_finish":"計画完了",
        "due": CONFIG["FIELD_LABELS"]["customer_due"],
        "hard_deadline": CONFIG["FIELD_LABELS"]["internal_deadline"],
        "progress":"進捗率"
    })
    st.write(show.to_html(escape=False, index=False), unsafe_allow_html=True)

# ====== ⑤ CSV 取り込み / 出力 ======
st.subheader("⑤ CSV 取り込み / 出力")

col_up, col_down = st.columns(2)

with col_up:
    st.markdown("**タスクCSVをインポート**（列例）")
    st.code(
        "item_code,stage,plan_start,plan_finish,act_start,act_finish,progress,owner,supplier,memo\n"
        "アイテムA,設計,2025-08-01,2025-08-14,2025-08-02,2025-08-10,1.0,山田,,\n"
        "アイテムA,外注製作品,2025-08-15,2025-09-05,,,,0.2,,協力会社A,\n",
        language="csv"
    )
    up = st.file_uploader("CSVファイルを選択", type=["csv"])
    if up:
        try:
            df = pd.read_csv(up)
            # 文字列→日付
            for c in ["plan_start","plan_finish","act_start","act_finish"]:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
            with closing(get_conn()) as con, con:
                for _, r in df.iterrows():
                    code = (str(r.get("item_code")) if pd.notna(r.get("item_code")) else "").strip()
                    if not code:
                        continue
                    row = pd.read_sql_query(
                        "SELECT id FROM items WHERE project_id=? AND code=?",
                        con, params=(project_id, code)
                    )
                    if row.empty:
                        con.execute("INSERT INTO items(project_id, code, description) VALUES(?,?,?)",
                                    (project_id, code, ""))
                        row = pd.read_sql_query(
                            "SELECT id FROM items WHERE project_id=? AND code=?",
                            con, params=(project_id, code)
                        )
                    iid = int(row.iloc[0]["id"])
                    con.execute("""
                        INSERT INTO tasks(item_id, stage, plan_start, plan_finish, act_start, act_finish, progress, owner, supplier, memo)
                        VALUES(?,?,?,?,?,?,?,?,?,?)
                    """, (
                        (r.get("stage") or "").strip(),
                        r.get("plan_start"),
                        r.get("plan_finish"),
                        r.get("act_start"),
                        r.get("act_finish"),
                        float(r.get("progress") or 0.0),
                        (r.get("owner") or "").strip(),
                        (r.get("supplier") or "").strip(),
                        (r.get("memo") or "").strip()
                    ))
            st.success("CSVを取り込みました。")
            st.rerun()
        except Exception as e:
            st.error(f"取り込みに失敗しました: {e}")

with col_down:
    exp_items = df_from_sql("SELECT * FROM items WHERE project_id=?", (project_id,))
    exp_tasks = df_from_sql("""
        SELECT i.code AS item_code, t.*
          FROM tasks t
          JOIN items i ON i.id = t.item_id
         WHERE i.project_id=?
         ORDER BY i.code, t.id
    """, (project_id,))
    st.download_button("アイテム一覧をCSVで出力", data=exp_items.to_csv(index=False), file_name="items.csv", mime="text/csv")
    st.download_button("タスク一覧をCSVで出力", data=exp_tasks.to_csv(index=False), file_name="tasks.csv", mime="text/csv")

st.caption("※ 背景は常に白で表示。顧客納期/社内締切を設定すると、逆算で『順調/要注意/遅延』を自動判定します。")
