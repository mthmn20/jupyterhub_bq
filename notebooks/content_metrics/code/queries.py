"""Module containing functions for programatically constructing queries to pull
various subject-level content analyses using tables in content_metrics."""


import datetime
import config as cfg


def _create_date_intervals():
    """Return a list of date pairs corresponding to first and last day of
    each month included in analysis."""
    max_date = datetime.datetime.strptime(LATEST_MONTH_END, "%Y-%m-%d")
    cur_date = datetime.datetime.strptime(ALL_TIME_START, "%Y-%m-%d")
    date_intervals = []
    while cur_date < max_date:
        pair = (cur_date.strftime("%Y-%m-%d"),
            (cur_date + datetime.timedelta(days=29)).strftime("%Y-%m-%d"))
        date_intervals.append(pair)
        next_month = cur_date + datetime.timedelta(days=31)
        cur_date = datetime.datetime(next_month.year, next_month.month, 1)
    return date_intervals


def _get_dates(all_time_start="2014-05-01"):
    """Return date constants for the dashboard."""
    today = datetime.datetime.now()
    last_month = datetime.datetime(
        today.year, today.month, 1) - datetime.timedelta(days=1)
    latest_month_start = datetime.datetime(
        last_month.year, last_month.month, 1).strftime("%Y-%m-%d")
    latest_month_end = datetime.datetime(
        last_month.year, last_month.month, 30).strftime("%Y-%m-%d")
    all_time_start_date = datetime.datetime.strptime(all_time_start, "%Y-%m-%d")
    return latest_month_start, latest_month_end, last_month.year, last_month.month, all_time_start, all_time_start_date.month, all_time_start_date.year

# Create constants available to rest of the query module.
LATEST_MONTH_START, LATEST_MONTH_END, CURR_YEAR, CURR_MONTH, ALL_TIME_START, ALL_TIME_START_MONTH,  ALL_TIME_START_YEAR = _get_dates()


def _create_where_clause(comparison_data):
    """Create a where clause that limits to the subjects/domain in the node."""
    domain = comparison_data.get('domain', None)
    subject = comparison_data.get('subject', None)
    subject_list = comparison_data.get('subject_list', None)
    if domain is not None:
        where_clause = "WHERE domain == '%s'" % (domain)
    elif subject is not None:
        where_clause = "WHERE subject == '%s'" % (subject)
    elif subject_list is not None:
        where_clause = "WHERE subject in ('%s') " % ("', '".join(subject_list))
    return where_clause


def _create_case_statement(comparison_nodes):
    """Create a case statement that defines different content areas of interest
    for comparison."""
    case_clauses = []
    for c, comparison_data in comparison_nodes.iteritems():
        domain = comparison_data.get('domain', None)
        subject = comparison_data.get('subject', None)
        subject_list = comparison_data.get('subject_list', None)
        if domain is not None:
            when_clause = "WHEN domain == '%s' THEN '%s'" % (domain, c)
        elif subject is not None:
            when_clause = "WHEN subject == '%s' THEN '%s'" % (subject, c)
        elif subject_list is not None:
            when_clause = "WHEN subject in ('%s') THEN '%s'" % (
                "', '".join(subject_list), c)
        case_clauses.append(when_clause)
    return "CASE " + " \n".join(case_clauses) + " END as content_area"


def content_production_query():
    """Pull all data on content production."""
    query = """SELECT * FROM content_metrics.content_production
    WHERE TIMESTAMP(creation_date) <= TIMESTAMP('%s')""" % LATEST_MONTH_END
    return query


def construct_usage_query(comparison_nodes, kind="all"):
    """Construct query to pull main usage data (TLT, number of learners) from
       content_metrics.learning_log_*, broken down by content area."""
    case_statement = _create_case_statement(comparison_nodes)
    if kind == "all":
        kind_list = ["video", "exercise", "article",
                     "scratchpad", "talkthrough"]
    else:
        kind_list = [kind]
    query = """
    // final query for usage
    SELECT
      content_area,
      month,
      year,
      SUM(total_learning_time) / (60 * 60) AS TLT,
      EXACT_COUNT_DISTINCT(kaid) num_learners,
      SUM(num_learning_nodes) AS num_content_learned,
      SUM(prop_nodes_completed)/count(prop_nodes_completed) as avg_prop_completed
      //SUM(completed_one_node) as num_completers,
    FROM
    (SELECT
      month,
      %s,
      year,
      total_learning_time,
      kaid,
      num_learning_nodes,
      num_completed_nodes,
      prop_nodes_completed,
      //completed_one_node
    FROM
      %s)
    WHERE (month <= %s OR year <= %s) AND (month >= %s OR year >= %s)
    GROUP BY
      content_area, month, year
    HAVING
      content_area is not NULL
    """ % (case_statement,
           ", ".join(["content_metrics.learning_log_" + k for k in kind_list]),
           CURR_MONTH, CURR_YEAR, ALL_TIME_START_MONTH, ALL_TIME_START_YEAR)
    return query


def construct_youtube_query(comparison_nodes, date):
    """Construct query to pull video view/minutes data broken down by whether
    it is viewed on KA or YouTube."""
    case_statement = _create_case_statement(comparison_nodes)
    date_str = date.replace("-", "")
    query = """SELECT
       // query youtube stats
       content_area,
       product,
       sum(views) as views,
       sum(estimatedMinutesWatched) as minutes_watched,
       FROM (
            SELECT
              domain,
              %s,
              views,
              estimatedMinutesWatched,
              CASE
                WHEN insightPlaybackLocationType == "EMBEDDED" THEN "KA"
                ELSE "YT"
              END as product
            FROM (
              SELECT
                slug,
                views,
                estimatedMinutesWatched,
                insightPlaybackLocationType
              FROM (
                SELECT
                  video_id,
                  views,
                  estimatedMinutesWatched,
                  insightPlaybackLocationType
                FROM
                  youtube_api_stats.YouTube30DayVideoBreakdown_%s) yt
              JOIN EACH (
                SELECT
                  youtube_id,
                  readable_id AS slug
                FROM
                  latest_content.Video ) vid
              ON
                yt.video_id == vid.youtube_id) AS yt_data
            JOIN EACH (
              SELECT
                domain,
                subject,
                SUBSTR(node_slug, 3) as node_slug
              FROM
                latest_content.topic_tree
              WHERE
                kind == "Video" ) tt
            ON
              tt.node_slug == yt_data.slug)
        GROUP BY
            content_area, product
    """ % (case_statement, date_str)
    return query


def construct_core_academic_query(core_subjects):
    """Construct query to pull usage data for core academic subjects."""
    subjects = []
    for node in core_subjects:
        s = cfg.COMPARISON_NODES[node].get("subject", None)
        sl = cfg.COMPARISON_NODES[node].get("subject_list", None)
        if s is not None:
            subjects.append(s)
        elif sl is not None:
            subjects.extend(sl)
    query = """
    // query for aggregating across all core academic subjects
    SELECT
        month,
        year,
        kind as content_type
        SUM(total_learning_time) / (60 * 60) AS TLT,
        EXACT_COUNT_DISTINCT(kaid) as num_learners,
        SUM(num_learning_nodes) AS num_learning_actions,
        SUM(prop_nodes_completed)/count(prop_nodes_completed) as avg_prop_completed
    FROM
        content_metrics.learning_log_article,
        content_metrics.learning_log_exercise,
        content_metrics.learning_log_video,
        content_metrics.learning_log_scratchpad,
        content_metrics.learning_log_talkthrough,

    WHERE subject in ('%s') AND TIMESTAMP(DATE) <= TIMESTAMP('%s')
    GROUP BY month, year, kind
    """ % ("','".join(subjects), LATEST_MONTH_END)
    return query


def construct_new_learner_query(content_type, node_data, last_month=False):
    """Construct query to pull new learners relative to existing learners,
    here existing is defined either as all time or as from previous month."""
    where = _create_where_clause(node_data)
    if last_month:
        existing = "content_metrics.%s_users" % "last_month"
    else:
        existing = "content_metrics.%s_users" % "all_previous"
    if content_type == "all":
        source = """content_metrics.learning_log_article,
        content_metrics.learning_log_exercise,
        content_metrics.learning_log_video,
        content_metrics.learning_log_scratchpad,
        content_metrics.learning_log_talkthrough"""
    else:
        source = "content_metrics.learning_log_%s" % content_type
        
    query = """
     SELECT count(*) as num_returning
     FROM
        (SELECT
          kaid,
            FROM
              %s
              %s AND
              month == %s
              AND year == %s
            GROUP BY
              kaid) current
         INNER JOIN EACH
         (%s) previous
         ON previous.kaid == current.kaid

    """ % (source, where, CURR_MONTH, CURR_YEAR, existing)
    return query


def create_request_logs_visit_query(comparisons=cfg.COMPARISON_NODES, total=False):
    """Construct query to pull number of visitors by area."""
    case_statement = _create_case_statement(comparisons)
    if not total:
        query = """
            // query for visitors
            SELECT
              content_type,
              content_area,
              EXACT_COUNT_DISTINCT(bingo_id) as num_visitors,
            FROM(
            SELECT
              learning_events as events,
              bingo_id,
              %s,
              CASE
                WHEN content_type contains "article" THEN "article"
                WHEN content_type contains "exercise" THEN "exercise"
                WHEN content_type contains "video" THEN "video"
                WHEN content_type contains "scratchpad" THEN "scratchpad"
                ELSE content_type
              END AS content_type,
              CASE
                WHEN content_type contains "pageview" then 0 ELSE learning_events
              END AS learning_events,
              user
            FROM
              [content_metrics.request_log_summary]
            WHERE domain IS NOT NULL)
            GROUP BY content_type, content_area
            HAVING content_area IS NOT NULL

        """ % (case_statement)
    else:
        query = """
            SELECT
              content_area,
              EXACT_COUNT_DISTINCT(bingo_id) as num_visitors,
              "all" as  content_type,
            FROM(
            SELECT
              learning_events as events,
              bingo_id,
              %s,
              CASE
                WHEN content_type contains "pageview" then 0 ELSE learning_events
              END AS learning_events,
              user
            FROM
              [content_metrics.request_log_summary]
            WHERE domain IS NOT NULL)
            GROUP BY content_area
            HAVING content_area IS NOT NULL
        """ % (case_statement)
        
    return query


def construct_device_breakdown_query(comparisons=cfg.COMPARISON_NODES):
    """Query to pull number of learners broken down by language and device."""
    case_statement = _create_case_statement(comparisons)
    query = """
            SELECT
              content_area,
              language,
              device_type,
              "all" as content_type,
              EXACT_COUNT_DISTINCT(bingo_id) as num_users,
            FROM(
            SELECT
              learning_events as events,
              CASE
                  WHEN language IS NULL THEN "en" ELSE language
              END as language,
              device_type,
              bingo_id,
              %s,
              CASE
                WHEN content_type contains "pageview" then 0 ELSE learning_events
              END AS learning_events,
              user
            FROM
              [content_metrics.request_log_summary]
            WHERE domain IS NOT NULL)
            GROUP BY content_area, language, device_type
            HAVING content_area IS NOT NULL

        """ % (case_statement)
    return query

# create dictionary of queries we will use to generate data
query_dict = {}
query_dict['production_queries'] = content_production_query()
query_dict['usage_queries_all'] = construct_usage_query(
    cfg.COMPARISON_NODES, kind="all")
query_dict['usage_queries_exercise'] = construct_usage_query(
    cfg.COMPARISON_NODES, kind="exercise")
query_dict['usage_queries_video'] = construct_usage_query(
    cfg.COMPARISON_NODES, kind="video")
query_dict['usage_queries_article'] = construct_usage_query(
    cfg.COMPARISON_NODES, kind="article")
query_dict['usage_queries_scratchpad'] = construct_usage_query(
    cfg.COMPARISON_NODES, kind="scratchpad")
query_dict['usage_queries_talkthrough'] = construct_usage_query(
    cfg.COMPARISON_NODES, kind="talkthrough")
query_dict['yt_logs_query'] = construct_youtube_query(
    cfg.COMPARISON_NODES, LATEST_MONTH_START)
query_dict['core_usage_queries_all'] = construct_usage_query(
    cfg.SUMMARY_NODE, kind="all")
query_dict['core_usage_queries_exercise'] = construct_usage_query(
    cfg.SUMMARY_NODE, kind="exercise")
query_dict['core_usage_queries_video'] = construct_usage_query(
    cfg.SUMMARY_NODE, kind="video")
query_dict['core_usage_queries_article'] = construct_usage_query(
    cfg.SUMMARY_NODE, kind="article")
query_dict['core_usage_queries_scratchpad'] = construct_usage_query(
    cfg.SUMMARY_NODE, kind="scratchpad")
query_dict['core_usage_queries_talkthrough'] = construct_usage_query(
    cfg.SUMMARY_NODE, kind="talkthrough")
query_dict['request_logs_type_breakdown'] = create_request_logs_visit_query(
    total=False, comparisons=cfg.COMPARISON_NODES)
query_dict['request_logs_all_types'] = create_request_logs_visit_query(
    total=True, comparisons=cfg.COMPARISON_NODES)
query_dict['request_logs_device_query'] = construct_device_breakdown_query(
    comparisons=cfg.COMPARISON_NODES)
query_dict['core_request_logs_type_breakdown'] = create_request_logs_visit_query(
    total=False, comparisons=cfg.SUMMARY_NODE)
query_dict['core_request_logs_all_types'] = create_request_logs_visit_query(
    total=True, comparisons=cfg.SUMMARY_NODE)
query_dict['core_request_logs_device_query'] = construct_device_breakdown_query(
    comparisons=cfg.SUMMARY_NODE)
