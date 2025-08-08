import os
import sqlite3
from datetime import date, datetime, timedelta
from contextlib import closing

import pandas as pd
import streamlit as st
import plotly.express as px

# ====== UI 設定 ======
st.set_page_config(page_title="連鋳用金型｜工程進捗見える化", layout="wide")
st.markdown("""
<style>
    body, .stApp { background-color: #FFFFFF !important; }
    .late { color:#fff; background:#d9534f; padding:2px 6px; border-radius:4px; }
    .warn { color:#212529; background:#f0ad4e; padding:2px 6px; border-radius:4px; }
    .ok { color:#fff; background:#5cb85c; padding:2px 6px; border-radius:4px; }
</style>
""", unsafe_allow_html=True)

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
    code TEXT,             -- 例: 長辺フレーム / 短辺カセットPL / 銅板 等
    description TEXT,
    plan_start DATE,
    plan_finish DATE,
    ksl_due DATE,          -- KSL納期（PDFに合わせた用語）
    hard_deadline DATE,    -- デッドライン
    UNIQUE(project_id, code)
);

CREATE TABLE IF NOT EXISTS tasks(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,   -- 設計/外注製作品/購入部品/組立/検査/試運転/解体/出荷
    plan_start DATE,
    plan_finish DATE,
    act_start DATE,
    act_finish DATE,
    progress REAL DEFAULT 0.0, -- 0.0~1.0
    owner TEXT,
    supplier TEXT,         -- 外注先/購買先
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

# ====== 便利関数 ======
def risk_level(row: pd.Series) -> str:
    """予定 vs 実績からリスクを判定"""
    today = date.today()
    plan_finish = row.get("plan_finish")
    act_finish = row.get("act_finish")
    progress = float(row.get("progress") or 0.0)

    # 進捗率からの予測終了日（超ざっくり：線形）※実績開始がない場合は予定開始を基準に
    if progress > 0 and progress < 1 and plan_finish:
        plan_start = row.get("plan_start")
        if not plan_start:
            plan_start = today
        plan_days = max((pd.to_datetime(plan_finish) - pd.to_datetime(plan_start)).days, 1)
        spent_ratio = progress
        est_finish = today + timedelta(days=int(plan_days * (1 - spent_ratio)))
    elif progress >= 1:
        est_finish = act_finish or plan_finish
    else:
        est_finish = plan_finish

    # 余裕日数（KSL/デッドライン基準で厳しい方）
    due_candidates = [d for d in [row.get("ksl_due"), row.get("hard_deadline"), plan_finish] if pd.notna(d)]
    due = min(pd.to_datetime(x) for x in due_candidates) if due_candidates else None

    if not due or not est_finish:
        return "warn"  # 期日未設定は注意

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
        return pd.read_sql_query(sql, con, params=params, parse_dates=["plan_start","plan_finish","act_start","act_finish","ksl_due","hard_deadline"])

def execute(sql, params=()):
    with closing(get_conn()) as con, con:
        con.execute(sql, params)

# ====== サイドバー：プロジェクト選択/作成 ======
st.sidebar.header("プロジェクト")
proj_df = df_from_sql("SELECT * FROM projects ORDER BY id DESC")
proj_names = ["(新規作成)"] + proj_df["name"].tolist()
sel = st.sidebar.selectbox("選択", proj_names)

if sel == "(新規作成)":
    with st.sidebar.form("new_proj"):
        name = st.text_input("名称", value="KILA 三島光産株式会社 機工事業本部")
        customer = st.text_input("客先", value="株式会社神戸製鋼所")
        note = st.text_area("備考", value="PDFの工程票をベースに管理")
        submitted = st.form_submit_button("作成")
        if submitted and name.strip():
            execute("INSERT INTO projects(name, customer, note) VALUES(?,?,?)", (name, customer, note))
            st.experimental_rerun()
    project_row = None
else:
    project_row = proj_df.loc[proj_df["name"] == sel].iloc[0]
    st.sidebar.caption(f"客先: {project_row['customer']}")

# ====== 初回データ投入（オプション） ======
def seed_items(project_id: int):
    """最初のアイテムとステージを自動生成"""
    base_items = [
        ("長辺フレーム", "SUSメイン材・FR含む"),
        ("短辺カセットPL", "140/200/300/350/380/400厚を含む"),
        ("銅板", "長辺・短辺"),
        ("鋳型組立品", "本機/予備機"),
    ]
    today = date.today()
    for code, desc in base_items:
        execute("""
            INSERT OR IGNORE INTO items(project_id, code, description, plan_start, plan_finish)
            VALUES(?,?,?,?,?)
        """, (project_id, code, desc, today, today + timedelta(days=60)))
        # ステージ雛形
        item_id = df_from_sql("SELECT id FROM items WHERE project_id=? AND code=?", (project_id, code)).iloc[0]["id"]
        for i, stage in enumerate(DEFAULT_STAGES):
            execute("""
                INSERT INTO tasks(item_id, stage, plan_start, plan_finish, progress)
                VALUES(?,?,?,?,?)
            """, (item_id, stage, today + timedelta(days=7*i), today + timedelta(days=7*(i+1)), 0.0))

# ====== メイン ======
st.title("連続鋳造用金型・工程進捗 見える化")

if project_row is None:
    st.info("左のサイドバーからプロジェクトを作成してください。")
    st.stop()

project_id = int(project_row["id"])

# 初回の簡易シード
if st.sidebar.button("サンプル工程を投入"):
    seed_items(project_id)
    st.success("サンプル工程を投入しました。")
    st.experimental_rerun()

# ====== アイテム（部品）一覧 ======
st.subheader("① アイテム（部品）一覧 / 期日設定")
items = df_from_sql("SELECT * FROM items WHERE project_id=? ORDER BY id", (project_id,))
with st.expander("新規アイテムを追加", expanded=False):
    with st.form("add_item"):
        col1, col2, col3 = st.columns([2,2,2])
        with col1:
            code = st.text_input("アイテム名（例：長辺フレーム）")
            desc = st.text_input("説明", value="")
        with col2:
            plan_start = st.date_input("計画開始日", value=date.today())
            plan_finish = st.date_input("計画完了日", value=date.today()+timedelta(days=30))
        with col3:
            ksl_due = st.date_input("KSL納期（任意）", value=None)
            hard_deadline = st.date_input("デッドライン（任意）", value=None)
        submitted = st.form_submit_button("追加")
        if submitted and code.strip():
            execute("""
                INSERT OR IGNORE INTO items(project_id, code, description, plan_start, plan_finish, ksl_due, hard_deadline)
                VALUES(?,?,?,?,?,?,?)
            """, (project_id, code, desc, plan_start, plan_finish, ksl_due, hard_deadline))
            st.success("アイテムを追加しました。")
            st.experimental_rerun()

if not items.empty:
    # リスク表示用
    items_view = items.copy()
    items_view["risk"] = items_view.apply(risk_level, axis=1)
    items_view["状態"] = items_view["risk"].map({"late":"遅延","warn":"要注意","ok":"順調"})
    items_view["badge"] = items_view["risk"].map(badge)
    st.write(items_view[["code","plan_start","plan_finish","ksl_due","hard_deadline","状態"]].rename(columns={
        "code":"アイテム"
    }).to_html(escape=False, index=False), unsafe_allow_html=True)
else:
    st.warning("アイテムが未登録です。")

# ====== タスク（工程）編集 ======
st.subheader("② タスク（工程）編集")
if not items.empty:
    sel_item = st.selectbox("対象アイテム", items["code"].tolist())
    item_id = int(items.loc[items["code"] == sel_item].iloc[0]["id"])

    tasks = df_from_sql("SELECT * FROM tasks WHERE item_id=? ORDER BY id", (item_id,))
    with st.form("edit_tasks"):
        st.caption("行を選んで入力 → 下の保存を押してください。進捗率は 0.0〜1.0。")
        edited = st.data_editor(
            tasks,
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
                "id": None, "item_id": None
            },
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic"
        )
        if st.form_submit_button("保存"):
            with closing(get_conn()) as con, con:
                for _, r in edited.iterrows():
                    con.execute("""
                        UPDATE tasks
                        SET stage=?, plan_start=?, plan_finish=?, act_start=?, act_finish=?, progress=?, owner=?, supplier=?, memo=?
                        WHERE id=?
                    """, (r["stage"], r["plan_start"], r["plan_finish"], r["act_start"], r["act_finish"],
                          float(r["progress"] or 0.0), r["owner"], r["supplier"], r["memo"], int(r["id"])))
            st.success("保存しました。")
            st.experimental_rerun()
else:
    st.stop()

# ====== ③ ガントチャート（予定 vs 実績） ======
st.subheader("③ ガントチャート（予定 / 実績）")
def make_gantt(df: pd.DataFrame, mode="plan"):
    if df.empty: return None
    g = df.copy()
    g["ItemStage"] = sel_item + "｜" + g["stage"]
    if mode == "plan":
        g = g.dropna(subset=["plan_start","plan_finish"])
        g["Start"] = pd.to_datetime(g["plan_start"])
        g["Finish"] = pd.to_datetime(g["plan_finish"])
        color = "#5bc0de"
        title = "計画"
    else:
        g = g.dropna(subset=["act_start","act_finish"])
        g["Start"] = pd.to_datetime(g["act_start"])
        g["Finish"] = pd.to_datetime(g["act_finish"])
        color = "#5cb85c"
        title = "実績"
    fig = px.timeline(g, x_start="Start", x_end="Finish", y="ItemStage", color_discrete_sequence=[color])
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(height=380, margin=dict(l=10,r=10,t=40,b=10), title=f"{title}ガント")
    return fig

colA, colB = st.columns(2)
with colA:
    fig_plan = make_gantt(tasks, "plan")
    if fig_plan: st.plotly_chart(fig_plan, use_container_width=True)
with colB:
    fig_act = make_gantt(tasks, "act")
    if fig_act: st.plotly_chart(fig_act, use_container_width=True)

# ====== ④ アラート（KSL納期/デッドライン 逆算） ======
st.subheader("④ 遅延アラート")
alert_rows = []
for _, it in items.iterrows():
    tdf = df_from_sql("SELECT * FROM tasks WHERE item_id=?", (int(it["id"]),))
    if tdf.empty: continue
    # 代表は最終ステージの計画/実績
    final = tdf.sort_values("plan_finish").iloc[-1]
    row = {**it.to_dict(), **{c: final[c] for c in ["plan_start","plan_finish","act_start","act_finish","progress"]}}
    row["risk"] = risk_level(pd.Series(row))
    alert_rows.append(row)
alert = pd.DataFrame(alert_rows)
if alert.empty:
    st.info("アラート対象はありません。")
else:
    alert["badge"] = alert["risk"].map(badge)
    show = alert.sort_values("risk").assign(アイテム=alert["code"])[
        ["badge","アイテム","plan_finish","ksl_due","hard_deadline","progress"]
    ].rename(columns={"plan_finish":"計画完了","ksl_due":"KSL納期","hard_deadline":"デッドライン","progress":"進捗率"})
    st.write(show.to_html(escape=False, index=False), unsafe_allow_html=True)

# ====== ⑤ CSV取込/出力 ======
st.subheader("⑤ CSV 取り込み / 出力")
col1, col2 = st.columns(2)
with col1:
    up = st.file_uploader("タスクCSVをインポート（列例: item_code,stage,plan_start,plan_finish,act_start,act_finish,progress,owner,supplier,memo）", type=["csv"])
    if up:
        df = pd.read_csv(up)
        # 文字列→日付
        for c in ["plan_start","plan_finish","act_start","act_finish"]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
        for _, r in df.iterrows():
            # アイテム解決
            row = df_from_sql("SELECT id FROM items WHERE project_id=? AND code=?", (project_id, r["item_code"]))
            if row.empty:
                execute("INSERT INTO items(project_id, code, description) VALUES(?,?,?)",
                        (project_id, r["item_code"], ""))
                row = df_from_sql("SELECT id FROM items WHERE project_id=? AND code=?", (project_id, r["item_code"]))
            iid = int(row.iloc[0]["id"])
            execute("""
                INSERT INTO tasks(item_id, stage, plan_start, plan_finish, act_start, act_finish, progress, owner, supplier, memo)
                VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (iid, r["stage"], r.get("plan_start"), r.get("plan_finish"),
                  r.get("act_start"), r.get("act_finish"), float(r.get("progress", 0) or 0),
                  r.get("owner"), r.get("supplier"), r.get("memo")))
        st.success("CSVを取り込みました。")
        st.experimental_rerun()

with col2:
    exp_items = df_from_sql("SELECT * FROM items WHERE project_id=?", (project_id,))
    exp_tasks = df_from_sql("""
        SELECT i.code AS item_code, t.*
        FROM tasks t JOIN items i ON i.id=t.item_id
        WHERE i.project_id=? ORDER BY i.code, t.id
    """, (project_id,))
    st.download_button("アイテム一覧をCSVで出力", data=exp_items.to_csv(index=False), file_name="items.csv", mime="text/csv")
    st.download_button("タスク一覧をCSVで出力", data=exp_tasks.to_csv(index=False), file_name="tasks.csv", mime="text/csv")

st.caption("※ KSL納期やデッドラインを設定すると、逆算して遅延・要注意を自動判定します。")
