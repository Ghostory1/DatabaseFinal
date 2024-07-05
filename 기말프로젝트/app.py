from flask import Flask, render_template, request, url_for
from db_conn import open_db, close_db

app = Flask(__name__)

# 페이지 당 표시할 영화 개수 설정
MOVIES_PER_PAGE = 10

HANGUL_INDEXES = {
    'ㄱ': ('가', '나'),
    'ㄴ': ('나', '다'),
    'ㄷ': ('다', '라'),
    'ㄹ': ('라', '마'),
    'ㅁ': ('마', '바'),
    'ㅂ': ('바', '사'),
    'ㅅ': ('사', '아'),
    'ㅇ': ('아', '자'),
    'ㅈ': ('자', '차'),
    'ㅊ': ('차', '카'),
    'ㅋ': ('카', '타'),
    'ㅌ': ('타', '파'),
    'ㅍ': ('파', '하'),
    'ㅎ': ('하', '힣')
}

@app.route('/')
def home():
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * MOVIES_PER_PAGE

    title = request.args.get('title', '')
    director = request.args.get('director', '')
    year_start = request.args.get('year_start', '')
    year_end = request.args.get('year_end', '')
    status = request.args.getlist('status')
    m_type = request.args.getlist('m_type')
    genre = request.args.getlist('genre')
    index = request.args.get('index', '')

    if '전체선택' in status:
        status = ['개봉', '개봉예정', '개봉준비', '후반작업', '촬영진행', '촬영준비', '기타']
    
    if '전체선택' in m_type:
        m_type = ['장편', '단편', '옴니버스', '온라인전용', '기타']

    if '전체선택' in genre:
        genre = ['드라마', '코미디', '액션', '멜로/로멘스', '스릴러', '미스터리', '공포(호러)', '어드벤처', '범죄', '가족', '판타지', 'SF', '서부극(웨스턴)', '사극', '애니메이션', '다큐멘터리', '전쟁', '뮤지컬', '성인물(에로)', '공연', '기타']

    conn, cur = open_db()

    # 전체 영화 데이터 개수 조회
    count_query = """
    SELECT COUNT(DISTINCT m.m_id) as count
    FROM movie m
    LEFT JOIN genre g ON m.m_id = g.m_id
    LEFT JOIN movie_director md ON m.m_id = md.m_id
    LEFT JOIN director d ON md.d_id = d.d_id
    WHERE 1=1
    """
    params = []
    if title:
        count_query += " AND m.title LIKE %s"
        params.append(f'%{title}%')
    if director:
        count_query += " AND d.name LIKE %s"
        params.append(f'%{director}%')
    if year_start:
        count_query += " AND m.year >= %s"
        params.append(year_start)
    if year_end:
        count_query += " AND m.year <= %s"
        params.append(year_end)
    if status:
        count_query += " AND m.status IN (%s)" % ','.join(['%s'] * len(status))
        params.extend(status)
    if m_type:
        count_query += " AND m.m_type IN (%s)" % ','.join(['%s'] * len(m_type))
        params.extend(m_type)
    if genre:
        count_query += " AND g.genre IN (%s)" % ','.join(['%s'] * len(genre))
        params.extend(genre)
    if index:
        if index in HANGUL_INDEXES:
            start_char, end_char = HANGUL_INDEXES[index]
            count_query += " AND m.title >= %s AND m.title < %s"
            params.extend([start_char, end_char])
        else:
            count_query += " AND m.eng_title LIKE %s"
            params.append(f'{index}%')

    cur.execute(count_query, params)
    total_movies = cur.fetchone()['count']

    # 페이징을 위한 영화 데이터 조회
    query = """
    SELECT m.title, m.eng_title, m.year, m.country, m.m_type, m.status, m.company, g.genre, GROUP_CONCAT(d.name) as director
    FROM movie m
    LEFT JOIN genre g ON m.m_id = g.m_id
    LEFT JOIN movie_director md ON m.m_id = md.m_id
    LEFT JOIN director d ON md.d_id = d.d_id
    WHERE 1=1
    """
    params = []
    if title:
        query += " AND m.title LIKE %s"
        params.append(f'%{title}%')
    if director:
        query += " AND d.name LIKE %s"
        params.append(f'%{director}%')
    if year_start:
        query += " AND m.year >= %s"
        params.append(year_start)
    if year_end:
        query += " AND m.year <= %s"
        params.append(year_end)
    if status:
        query += " AND m.status IN (%s)" % ','.join(['%s'] * len(status))
        params.extend(status)
    if m_type:
        query += " AND m.m_type IN (%s)" % ','.join(['%s'] * len(m_type))
        params.extend(m_type)
    if genre:
        query += " AND g.genre IN (%s)" % ','.join(['%s'] * len(genre))
        params.extend(genre)
    if index:
        if index in HANGUL_INDEXES:
            start_char, end_char = HANGUL_INDEXES[index]
            query += " AND m.title >= %s AND m.title < %s"
            params.extend([start_char, end_char])
        else:
            query += " AND m.eng_title LIKE %s"
            params.append(f'{index}%')
    
    query += " GROUP BY m.m_id, m.title, m.eng_title, m.year, m.country, m.m_type, m.status, m.company, g.genre"
    query += " LIMIT %s OFFSET %s"
    params.extend([MOVIES_PER_PAGE, offset])
    
    cur.execute(query, params)
    movies = cur.fetchall()

    close_db(conn, cur)

    # 총 페이지 수 계산
    total_pages = (total_movies + MOVIES_PER_PAGE - 1) // MOVIES_PER_PAGE

    return render_template('index.html', movies=movies, page=page, total_pages=total_pages, max=max, min=min, title=title, director=director, year_start=year_start, year_end=year_end, status=status, m_type=m_type, genre=genre, index=index, str=str)

if __name__ == "__main__":
    app.run(debug=True)
