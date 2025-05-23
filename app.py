# ⚔️  AB Battle • Multi-language (zh/en) • No-ID CSV
import streamlit as st
import pandas as pd, sqlite3, random, io, re
from datetime import datetime, timezone

# ───── 常量 ──────────────────────────────────────────────────────────────
CSV_PATH = "comparison_results.csv"
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

    # 调试信息：检查文本长度 - 修改为同时支持中英文
    st.sidebar.write(f"{'中文' if lang == 'zh' else 'English'} {'文本样本数量' if lang == 'zh' else 'sample count'}: {len(df)}")
    if len(df) > 0:
        sample_len_a = df[col_a].str.len().mean()
        sample_len_b = df[col_b].str.len().mean()
        if lang == "zh":
            st.sidebar.write(f"版本A平均长度: {sample_len_a:.1f}字")
            st.sidebar.write(f"版本B平均长度: {sample_len_b:.1f}字")
        else:
            st.sidebar.write(f"Version A avg length: {sample_len_a:.1f} chars")
            st.sidebar.write(f"Version B avg length: {sample_len_b:.1f} chars")

    # 筛选两列都非空的行
    df = df[df[col_a].notna() & df[col_b].notna() & (df[col_a] != '') & (df[col_b] != '')].copy()
    
    # 重置索引并生成id
    df = df.reset_index(drop=True)
    df.insert(0, "id", (df.index + 1).astype(str))          # id = "1", "2", …
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
                # 重置语言选择，确保登录后会显示语言选择界面
                if "lang" in st.session_state:
                    del st.session_state.lang
            else:
                st.warning("邮箱格式不正确")
    st.stop()

# —— 语言选择 ——  
if "lang" not in st.session_state:
    st.title("⚔️  AB Battle — 选择语言 / Choose Language")
    st.session_state.lang = st.selectbox(
        "请选择语言 / Choose language",
        options=list(LANG_CHOICES.keys()),
        format_func=lambda k: "中文" if k == "zh" else "English")
    
    if st.button("确认 / Confirm"):
        st.rerun()
    st.stop()  # 添加这行确保在选择语言前不会继续执行

email = st.session_state.email
lang  = st.session_state.lang

# 根据语言设置显示不同的界面文本
if lang == "zh":
    sidebar_info = f"🆔 {email}\n🌐 语言: 中文"
    switch_lang_text = "🔄 切换语言 / Switch Language"
    tab_labels = ["📝 标注任务", "📊 排行榜 / 进度"]
    
    # 中文界面文本
    complete_text = "🎉 本语言全部完成！"
    time_text = lambda sec, pct: f"🏆 你用时 **{int(sec)} 秒**，击败了 **{pct:.1f}%** 的同语言同事！"
    download_text = "📥 下载我的标注 CSV"
    question_text = lambda done, total: f"题目 {done}/{total}"
    version_a_text = "🔵 版本A"
    version_b_text = "🔵 版本B"
    vote_a_text = "👍 版本A 更好"
    tie_text = "🤝 平分"
    vote_b_text = "👍 版本B 更好"
    leaderboard_text = "⏱ 标注速度排行榜"
    no_data_text = "暂无标注数据"
    export_text = "📊 数据导出"
    current_lang_text = lambda l: f"当前语言({l})已收集标注数据:"
    download_stats_text = "📥 下载题目评分统计CSV"
    download_raw_text = "📥 下载所有原始标注数据"
    admin_text = "管理员功能"
    download_all_text = "📥 导出所有语言的标注数据"
    no_export_text = "暂无标注数据可导出"
    rank_text = lambda rank, total, count, time: f"🏆 你的排名: 第**{rank}**名 / 共{total}人，已完成**{count}**题，平均每题用时**{time}**秒"
    
else:  # English
    sidebar_info = f"🆔 {email}\n🌐 Language: English"
    switch_lang_text = "🔄 Switch Language / 切换语言"
    tab_labels = ["📝 Annotation Task", "📊 Leaderboard / Progress"]
    
    # 英文界面文本
    complete_text = "🎉 All completed for this language!"
    time_text = lambda sec, pct: f"🏆 You completed in **{int(sec)} seconds**, beating **{pct:.1f}%** of your colleagues in the same language!"
    download_text = "📥 Download My Annotations CSV"
    question_text = lambda done, total: f"Question {done}/{total}"
    version_a_text = "🔵 Version A"
    version_b_text = "🔵 Version B"
    vote_a_text = "👍 Version A is better"
    tie_text = "🤝 Tie"
    vote_b_text = "👍 Version B is better"
    leaderboard_text = "⏱ Annotation Speed Leaderboard"
    no_data_text = "No annotation data available"
    export_text = "📊 Data Export"
    current_lang_text = lambda l: f"Collected annotation data for current language ({l}):"
    download_stats_text = "📥 Download Question Stats CSV"
    download_raw_text = "📥 Download All Raw Annotation Data"
    admin_text = "Admin Functions"
    download_all_text = "📥 Export Annotations for All Languages"
    no_export_text = "No annotation data available for export"
    rank_text = lambda rank, total, count, time: f"🏆 Your rank: **{rank}** out of {total}, completed **{count}** questions, average time per question: **{time}** seconds"

st.sidebar.info(sidebar_info)

# 添加语言切换按钮
with st.sidebar:
    if st.button(switch_lang_text):
        # 清除缓存的数据
        st.cache_data.clear()
        # 删除语言设置
        if "lang" in st.session_state:
            del st.session_state.lang
        # 强制重新运行
        st.rerun()

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
tab_vote, tab_board = st.tabs(tab_labels)

# —— Tab · 标注 ——  
with tab_vote:
    if not remaining:
        st.success(complete_text)

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

        st.markdown(time_text(my_sec, pct))

        csv_buf = io.StringIO()
        pd.read_sql("""
            SELECT pair_id,label,ts FROM votes
            WHERE email=? AND lang=?""", conn, params=(email, lang)
        ).to_csv(csv_buf, index=False)
        st.download_button(download_text, csv_buf.getvalue(),
                           file_name=f"votes_{lang}_{email}.csv", mime="text/csv")
    else:
        pair = remaining[0]
        st.header(question_text(len(done_ids)+1, total))

        c1, c2 = st.columns(2, gap="large")
        with c1:
            st.subheader(version_a_text)
            # 使用markdown并设置容器样式
            st.markdown(
                f"""<div style="height:400px;overflow-y:auto;border:1px solid #eee;padding:10px;border-radius:5px;">
                {pair["answer_a"]}
                </div>""", 
                unsafe_allow_html=True
            )
        with c2:
            st.subheader(version_b_text)
            st.markdown(
                f"""<div style="height:400px;overflow-y:auto;border:1px solid #eee;padding:10px;border-radius:5px;">
                {pair["answer_b"]}
                </div>""", 
                unsafe_allow_html=True
            )

        st.divider()

        def vote(label:int):
            conn.execute("INSERT OR REPLACE INTO votes VALUES (?,?,?,?,?)",
                         (email, lang, pair["id"], label, utcnow()))
            conn.commit()
            if len(done_ids)+1 == total:      # 最后一题
                conn.execute("UPDATE users SET finish_ts=? WHERE email=? AND lang=?",
                             (utcnow(), email, lang))
                conn.commit()
            # Instead of st.rerun(), set a flag in session state
            st.session_state.need_rerun = True

        # Check if we need to rerun at the top level
        if st.session_state.get('need_rerun', False):
            st.session_state.need_rerun = False
            st.rerun()

        b1, b2, b3 = st.columns([1,1,1], gap="large")
        b1.button(vote_a_text, on_click=vote, args=(1,), use_container_width=True)
        b2.button(tie_text, on_click=vote, args=(0,), use_container_width=True)
        b3.button(vote_b_text, on_click=vote, args=(-1,), use_container_width=True)

        # 修复进度条问题
        progress_value = min((len(done_ids)+1)/total, 1.0)
        st.progress(progress_value, text=f"{len(done_ids)+1}/{total}")

# —— Tab · 排行榜 ——  
with tab_board:
    st.subheader(leaderboard_text)
    
    # 获取每个用户的标注数据和时间
    df_votes = pd.read_sql("""
        SELECT email, lang, COUNT(*) as done_count, 
               MIN(ts) as first_ts, MAX(ts) as last_ts
        FROM votes
        WHERE lang=?
        GROUP BY email, lang
        HAVING done_count > 0
    """, conn, params=(lang,))
    
    if not df_votes.empty:
        # 计算每个用户的标注速度（每题平均用时）
        df_votes['first_ts'] = pd.to_datetime(df_votes['first_ts'])
        df_votes['last_ts'] = pd.to_datetime(df_votes['last_ts'])
        df_votes['total_seconds'] = (df_votes['last_ts'] - df_votes['first_ts']).dt.total_seconds()
        # 避免除零错误，至少计算为1秒
        df_votes['total_seconds'] = df_votes['total_seconds'].apply(lambda x: max(x, 1))
        # 计算平均每题用时（秒）
        df_votes['avg_seconds_per_question'] = df_votes['total_seconds'] / df_votes['done_count']
        
        # 排序并显示
        df_board = df_votes.sort_values('avg_seconds_per_question')
        
        # 根据语言设置显示不同的列名
        if lang == "zh":
            columns = ['排名', '邮箱', '已完成题数', '平均每题用时(秒)']
            df_board['排名'] = range(1, len(df_board) + 1)
            df_board['已完成题数'] = df_board['done_count']
            df_board['平均每题用时(秒)'] = df_board['avg_seconds_per_question'].round(1)
            df_board['邮箱'] = df_board['email']
        else:
            columns = ['Rank', 'Email', 'Completed', 'Avg Time(s)']
            df_board['Rank'] = range(1, len(df_board) + 1)
            df_board['Completed'] = df_board['done_count']
            df_board['Avg Time(s)'] = df_board['avg_seconds_per_question'].round(1)
            df_board['Email'] = df_board['email']
        
        st.dataframe(df_board[columns], hide_index=True)
        
        # 显示当前用户的排名
        if email in df_board['email'].values:
            my_rank = df_board[df_board['email'] == email]['Rank' if lang == 'en' else '排名'].values[0]
            my_time = df_board[df_board['email'] == email]['Avg Time(s)' if lang == 'en' else '平均每题用时(秒)'].values[0]
            my_count = df_board[df_board['email'] == email]['Completed' if lang == 'en' else '已完成题数'].values[0]
            total_users = len(df_board)
            st.success(rank_text(my_rank, total_users, my_count, my_time))
    else:
        st.info(no_data_text)
    
    # 添加数据导出功能
    st.divider()
    st.subheader(export_text)
    
    # 获取所有标注数据
    df_all_votes = pd.read_sql("""
        SELECT v.pair_id, v.email, v.lang, v.label, v.ts
        FROM votes v
        WHERE v.lang=?
    """, conn, params=(lang,))
    
    if not df_all_votes.empty:
        # 计算每个题目的平均得分和投票情况
        if lang == "zh":
            df_stats = df_all_votes.groupby('pair_id').agg(
                投票人数=('email', 'count'),
                平均得分=('label', 'mean'),
                版本A更好=('label', lambda x: sum(x == 1)),
                平分=('label', lambda x: sum(x == 0)),
                版本B更好=('label', lambda x: sum(x == -1))
            ).reset_index()
        else:
            df_stats = df_all_votes.groupby('pair_id').agg(
                Voters=('email', 'count'),
                AvgScore=('label', 'mean'),
                VersionA=('label', lambda x: sum(x == 1)),
                Tie=('label', lambda x: sum(x == 0)),
                VersionB=('label', lambda x: sum(x == -1))
            ).reset_index()
        
        # 显示统计结果
        st.write(current_lang_text(lang))
        st.dataframe(df_stats, hide_index=True)
        
        # 导出CSV按钮
        csv_buf = io.StringIO()
        df_stats.to_csv(csv_buf, index=False)
        st.download_button(
            download_stats_text, 
            csv_buf.getvalue(),
            file_name=f"question_stats_{lang}.csv", 
            mime="text/csv"
        )
        
        # 导出原始数据
        csv_buf_raw = io.StringIO()
        df_all_votes.to_csv(csv_buf_raw, index=False)
        st.download_button(
            download_raw_text, 
            csv_buf_raw.getvalue(),
            file_name=f"all_votes_{lang}.csv", 
            mime="text/csv"
        )
        
        # 管理员功能 - 导出所有语言的数据
        with st.expander(admin_text):
            all_langs_data = pd.read_sql("""
                SELECT v.pair_id, v.email, v.lang, v.label, v.ts
                FROM votes v
            """, conn)
            
            if not all_langs_data.empty:
                csv_buf_all = io.StringIO()
                all_langs_data.to_csv(csv_buf_all, index=False)
                st.download_button(
                    download_all_text, 
                    csv_buf_all.getvalue(),
                    file_name="all_languages_votes.csv", 
                    mime="text/csv"
                )
    else:
        st.info(no_export_text)
