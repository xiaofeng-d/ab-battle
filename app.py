# ⚔️  AB Battle • Multi-language (zh/en) • No-ID CSV
import streamlit as st
import pandas as pd, sqlite3, random, io, re
from datetime import datetime, timezone

# ───── 常量 ──────────────────────────────────────────────────────────────
CSV_PATH = "pairs.csv"
DB_PATH  = "votes.db"
LANG_CHOICES = {
    "zh": ("normal_conversation_history_zh", "personalized_conversation_history_zh"),
    "en": ("normal_conversation_history_en", "personalized_conversation_history_en"),
}

# ───── 工具函数 ──────────────────────────────────────────────────────────
def utcnow():
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")

@st.cache_data
def load_pairs(path: str, lang: str):
    """读取 CSV → 取指定语言两列 → 用行号生成 id → 随机打乱"""
    col_a, col_b = LANG_CHOICES[lang]
    df = pd.read_csv(path, dtype=str)

    # 检查缺列
    for c in (col_a, col_b):
        if c not in df.columns:
            st.error(f"CSV 缺少列: {c}")
            st.stop()

    df = df[[col_a, col_b]].copy()
    df.insert(0, "id", (df.index + 1).astype(str))          # id = “1”, “2”, …
    df.rename(columns={col_a: "answer_a", col_b: "answer_b"}, inplace=True)

    recs = df.to_dict("records")
    random.shuffle(recs)
    return recs

def init_db(path=DB_PATH):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS votes (
        email TEXT,
        lang  TEXT,
        pair_id TEXT,
        label INTEGER,
        ts TEXT,
        PRIMARY KEY (email, lang, pair_id)
      )""")
    conn.execute("""
      CREATE TABLE IF NOT EXISTS users (
        email TEXT,
        lang  TEXT,
        start_ts TEXT,
        finish_ts TEXT,
        PRIMARY KEY (email, lang)
      )""")
    conn.commit()
    return conn

# ───── 初始化 ───────────────────────────────────────────────────────────
conn = init_db()
email_cloud = getattr(st.experimental_user, "email", None)
if email_cloud:
    st.session_state.email = email_cloud.lower()

# —— 登录 ——  
if "email" not in st.session_state:
    EMAIL_RE = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    with st.form("login"):
        st.title("⚔️  AB Battle — 登录")
        raw = st.text_input("请输入邮箱开始", placeholder="you@company.com")
        if st.form_submit_button("进入"):
            if EMAIL_RE.match(raw.strip()):
                st.session_state.email = raw.strip().lower()
            else:
                st.warning("邮箱格式不正确")
    st.stop()

# —— 语言选择 ——  
if "lang" not in st.session_state:
    st.session_state.lang = st.selectbox(
        "请选择语言 / Choose language",
        options=list(LANG_CHOICES.keys()),
        format_func=lambda k: "中文" if k == "zh" else "English")
    st.experimental_rerun()

email = st.session_state.email
lang  = st.session_state.lang
st.sidebar.info(f"🆔 {email}\n🌐 语言: {lang}")

# —— 首次进入记录开始时间 ——  
conn.execute("INSERT OR IGNORE INTO users(email,lang,start_ts) VALUES (?,?,?)",
             (email, lang, utcnow()))
conn.commit()

# —— 读取题库 & 进度 ——  
pairs  = load_pairs(CSV_PATH, lang)
total  = len(pairs)
done_ids = {r[0] for r in conn.execute(
    "SELECT pair_id FROM votes WHERE email=? AND lang=?", (email, lang))}
remaining = [p for p in pairs if p["id"] not in done_ids]

# ───── 页面 Tabs ────────────────────────────────────────────────────────
tab_vote, tab_board = st.tabs(["📝 标注任务", "📊 排行榜 / 进度"])

# —— Tab · 标注 ——  
with tab_vote:
    if not remaining:
        st.success("🎉 本语言全部完成！")

        # 计算用时 & 击败百分比
        df_u = pd.read_sql("""
            SELECT start_ts, finish_ts FROM users
            WHERE email=? AND lang=?""", conn, params=(email, lang))
        t0, t1 = pd.to_datetime(df_u.start_ts[0]), pd.to_datetime(df_u.finish_ts[0])
        my_sec = (t1 - t0).total_seconds()

        df_all = pd.read_sql("""
            SELECT (julianday(finish_ts)-julianday(start_ts))*86400 AS s
            FROM users WHERE lang=? AND finish_ts IS NOT NULL""", conn, params=(lang,))
        pct = 100 * (df_all.s < my_sec).sum() / len(df_all)

        st.markdown(f"🏆 你用时 **{int(my_sec)} 秒**，击败了 **{pct:.1f}%** 的同语言同事！")

        csv_buf = io.StringIO()
        pd.read_sql("""
            SELECT pair_id,label,ts FROM votes
            WHERE email=? AND lang=?""", conn, params=(email, lang)
        ).to_csv(csv_buf, index=False)
        st.download_button("📥 下载我的标注 CSV", csv_buf.getvalue(),
                           file_name=f"votes_{lang}_{email}.csv", mime="text/csv")
    else:
        pair = remaining[0]
        st.header(f"题目 {len(done_ids)+1}/{total}")

        c1, c2 = st.columns(2, gap="large")
        with c1:
            st.subheader("🔵 Normal")
            st.write(pair["answer_a"])
        with c2:
            st.subheader("🟢 Personalized")
            st.write(pair["answer_b"])

        st.divider()

        def vote(label:int):
            conn.execute("INSERT OR REPLACE INTO votes VALUES (?,?,?,?,?)",
                         (email, lang, pair["id"], label, utcnow()))
            conn.commit()
            if len(done_ids)+1 == total:      # 最后一题
                conn.execute("UPDATE users SET finish_ts=? WHERE email=? AND lang=?",
                             (utcnow(), email, lang))
                conn.commit()
            st.experimental_rerun()

        b1, b2, b3 = st.columns([1,1,1], gap="large")
        b1.button("👍 Normal 更好",        on_click=vote, args=(1,),  use_container_width=True)
        b2.button("🤝 平分",              on_click=vote, args=(0,),  use_container_width=True)
        b3.button("👍 Personalized 更好", on_click=vote, args=(-1,), use_container_width=True)

        st.progress((len(done_ids)+1)/total, text=f"{len(done_ids)+1}/{total}")

# —— Tab · 排行榜 ——  
with tab_board:
    st.subheader("⏱ 用时排行榜")
    df_board = pd.read_sql(
