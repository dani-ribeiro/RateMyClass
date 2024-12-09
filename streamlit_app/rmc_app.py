import streamlit as st
import snowflake.connector
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from snowflake_info import SnowflakeInfo

# Snowflake connection function
def get_data_from_snowflake(query):
    conn = snowflake.connector.connect(
        user=f'{SnowflakeInfo.USERNAME}',
        password=f'{SnowflakeInfo.PASSWORD}',
        account=f'{SnowflakeInfo.ACCOUNT}',
        database=f'{SnowflakeInfo.DATABASE}',
        schema=f'{SnowflakeInfo.SCHEMA}'
    )
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return pd.DataFrame(data, columns=columns)

query_quality_by_department = """
SELECT 
    d.department_name AS department_name, 
    AVG(f.QUALITY) AS avg_quality, 
FROM FACT_REVIEW f
JOIN DIM_DEPARTMENT d ON f.DEPARTMENT_ID = d.DEPARTMENT_ID
WHERE f.QUALITY IS NOT NULL
GROUP BY d.department_name
ORDER BY 2 DESC;
"""
df_quality = get_data_from_snowflake(query_quality_by_department)
st.title("WashU RMC Dashboard")
st.subheader("Average Quality by Department")
fig1, ax1 = plt.subplots(figsize=(15, 6))
ax1.bar(df_quality['DEPARTMENT_NAME'], df_quality['AVG_QUALITY'], color="skyblue")
ax1.set_xlabel("Department")
ax1.set_ylabel("Avg Quality")
ax1.set_xticks(range(len(df_quality['DEPARTMENT_NAME'])))
ax1.set_xticklabels(df_quality['DEPARTMENT_NAME'], rotation=45, ha='right', fontsize=8)
st.pyplot(fig1)

query_difficulty_by_department = """
SELECT 
    d.department_name AS department_name, 
    AVG(f.DIFFICULTY) AS avg_difficulty, 
FROM FACT_REVIEW f
JOIN DIM_DEPARTMENT d ON f.DEPARTMENT_ID = d.DEPARTMENT_ID
WHERE f.DIFFICULTY IS NOT NULL
GROUP BY d.department_name
ORDER BY 2 DESC;
"""
df_difficulty = get_data_from_snowflake(query_difficulty_by_department)
st.subheader("Average Difficulty by Department")
fig2, ax2 = plt.subplots(figsize=(15, 6))
ax2.bar(df_difficulty['DEPARTMENT_NAME'], df_difficulty['AVG_DIFFICULTY'], color="red")
ax2.set_xlabel("Department")
ax2.set_ylabel("Avg Difficulty")
ax2.set_xticks(range(len(df_difficulty['DEPARTMENT_NAME'])))
ax2.set_xticklabels(df_difficulty['DEPARTMENT_NAME'], rotation=45, ha='right', fontsize=8)
st.pyplot(fig2)

query_quality_over_time = """
SELECT
    DATE_TRUNC('month', DATE) AS month, 
    AVG(QUALITY) AS avg_quality
FROM FACT_REVIEW
WHERE QUALITY IS NOT NULL
GROUP BY month
ORDER BY month;
"""
df_quality_time = get_data_from_snowflake(query_quality_over_time)

query_difficulty_over_time = """
SELECT 
    DATE_TRUNC('month', DATE) AS month, 
    AVG(DIFFICULTY) AS avg_difficulty
FROM FACT_REVIEW
WHERE DIFFICULTY IS NOT NULL
GROUP BY month
ORDER BY month;
"""
df_difficulty_time = get_data_from_snowflake(query_difficulty_over_time)

st.subheader("Average Quality and Difficulty Over Time")
fig3, ax3 = plt.subplots()
ax3.plot(df_quality_time['MONTH'], df_quality_time['AVG_QUALITY'], label='Avg Quality')
ax3.plot(df_difficulty_time['MONTH'], df_difficulty_time['AVG_DIFFICULTY'], label='Avg Difficulty')
ax3.legend()
st.pyplot(fig3)


query_sentiment_time = """
SELECT 
    DATE_TRUNC('month', DATE) AS month, 
    AVG(SENTIMENT_SCORE) AS avg_sentiment
FROM FACT_REVIEW
GROUP BY month
ORDER BY month;
"""
df_sentiment_time = get_data_from_snowflake(query_sentiment_time)

st.subheader("Average Sentiment Score Over Time")
fig4, ax4 = plt.subplots()
ax4.plot(df_sentiment_time['MONTH'], df_sentiment_time['AVG_SENTIMENT'], label="Avg Sentiment", color="blue")
ax4.set_ylim([-1, 1])
ax4.set_xlabel("Month")
ax4.set_ylabel("Sentiment Score")
ax4.legend()
st.pyplot(fig4)

query_grade_distribution = """
SELECT 
    GRADE, 
    COUNT(*) AS count
FROM FACT_REVIEW
WHERE GRADE IS NOT NULL
GROUP BY GRADE
ORDER BY count DESC;
"""
df_grades = get_data_from_snowflake(query_grade_distribution)
df_grades['PERCENTAGE'] = df_grades['COUNT'] / df_grades['COUNT'].sum() * 100
df_grades['GRADE'] = np.where(df_grades['PERCENTAGE'] < 1.0, 'Other', df_grades['GRADE'])
df_grades_grouped = df_grades.groupby('GRADE', as_index=False).agg({'COUNT': 'sum'})
st.subheader("Grade Distribution")
fig5, ax5 = plt.subplots()
ax5.pie(df_grades_grouped['COUNT'], labels=df_grades_grouped['GRADE'], autopct='%1.1f%%', textprops={'fontsize': 10})
st.pyplot(fig5)

# Step 1: Fetch all departments
query_departments = """
SELECT DISTINCT department_name 
FROM DIM_DEPARTMENT
ORDER BY department_name;
"""
df_departments = get_data_from_snowflake(query_departments)

# Step 2: Department dropdown
selected_department = st.selectbox("Select a Department", df_departments['DEPARTMENT_NAME'])

# Step 3: Fetch data for selected department
if selected_department:
    # Top-quality classes
    query_top_quality = f"""
    SELECT 
        c.course_code AS course_code, 
        AVG(r.QUALITY) AS avg_quality
    FROM FACT_REVIEW AS r
    JOIN DIM_CLASS AS c ON r.CLASS_ID = c.CLASS_ID
    JOIN DIM_DEPARTMENT AS d ON r.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.DEPARTMENT_NAME = '{selected_department}'
        AND r.DATE >= DATEADD(year, -5, CURRENT_DATE)
    GROUP BY course_code
    ORDER BY avg_quality DESC
    LIMIT 5;
    """
    df_top_quality = get_data_from_snowflake(query_top_quality)

    # Top-difficulty classes
    query_top_difficulty = f"""
    SELECT 
        c.course_code AS course_code, 
        AVG(r.DIFFICULTY) AS avg_difficulty
    FROM FACT_REVIEW AS r
    JOIN DIM_CLASS AS c ON r.CLASS_ID = c.CLASS_ID
    JOIN DIM_DEPARTMENT AS d ON r.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.DEPARTMENT_NAME = '{selected_department}'
    GROUP BY course_code
    ORDER BY avg_difficulty DESC
    LIMIT 5;
    """
    df_top_difficulty = get_data_from_snowflake(query_top_difficulty)

    # Top professors
    query_top_professors_by_quality = f"""
    SELECT 
        p.professor_name AS professor_name, 
        AVG(r.QUALITY) AS avg_quality,
        COUNT(r.REVIEW_ID) AS review_count
    FROM FACT_REVIEW AS r
    JOIN DIM_PROFESSOR AS p ON r.PROFESSOR_ID = p.PROFESSOR_ID
    JOIN DIM_DEPARTMENT AS d ON r.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.DEPARTMENT_NAME = '{selected_department}'
        AND r.DATE >= DATEADD(year, -5, CURRENT_DATE)
    GROUP BY professor_name
    ORDER BY avg_quality DESC, review_count DESC
    LIMIT 5;
    """
    df_top_professors_by_quality = get_data_from_snowflake(query_top_professors_by_quality)

    query_top_professors_easiness = f"""
    SELECT 
        p.professor_name AS professor_name, 
        AVG(r.DIFFICULTY) AS avg_difficulty,
        COUNT(r.REVIEW_ID) AS review_count
    FROM FACT_REVIEW AS r
    JOIN DIM_PROFESSOR AS p ON r.PROFESSOR_ID = p.PROFESSOR_ID
    JOIN DIM_DEPARTMENT AS d ON r.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.DEPARTMENT_NAME = '{selected_department}'
        AND r.DATE >= DATEADD(year, -5, CURRENT_DATE)
    GROUP BY professor_name
    ORDER BY avg_difficulty ASC, review_count DESC
    LIMIT 5;
    """
    df_top_professors_easiness = get_data_from_snowflake(query_top_professors_easiness)

    # Sentiment within department
    query_sentiment = f"""
    SELECT 
        AVG(SENTIMENT_SCORE) AS avg_sentiment
    FROM FACT_REVIEW AS r 
    JOIN DIM_DEPARTMENT AS d ON r.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.DEPARTMENT_NAME = '{selected_department}'
        AND r.DATE >= DATEADD(year, -5, CURRENT_DATE);
    """
    df_sentiment_avg = get_data_from_snowflake(query_sentiment)

    query_sentiment_trend = f"""
    SELECT 
        YEAR(r.DATE) AS year, 
        AVG(r.SENTIMENT_SCORE) AS avg_sentiment
    FROM FACT_REVIEW AS r
    JOIN DIM_DEPARTMENT AS d ON r.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.DEPARTMENT_NAME = '{selected_department}'
        AND r.DATE >= DATEADD(year, -5, CURRENT_DATE)
    GROUP BY year
    ORDER BY year;
    """
    df_sentiment_trend = get_data_from_snowflake(query_sentiment_trend)

    # Display department-level data
    st.subheader(f"Top Quality Classes in {selected_department} in Past 5 Years")
    st.write(df_top_quality)

    st.subheader(f"Top Difficulty Classes in {selected_department} in Past 5 Years")
    st.write(df_top_difficulty)

    st.subheader(f"Top Quality Professors in {selected_department} in Past 5 Years")
    st.write(df_top_professors_by_quality)

    st.subheader(f"Top Easiest Professors in {selected_department} in Past 5 Years")
    st.write(df_top_professors_easiness)

    st.subheader(f"Sentiment in {selected_department} in Past 5 Years")
    st.metric("Average Sentiment", f"{df_sentiment_avg['AVG_SENTIMENT'][0]:.2f}")

    # Sentiment trend plot
    fig1, ax1 = plt.subplots()
    ax1.plot(df_sentiment_trend['YEAR'], df_sentiment_trend['AVG_SENTIMENT'], label="Sentiment Over Time")
    ax1.axhline(df_sentiment_avg['AVG_SENTIMENT'][0], color="red", linestyle="--", label="Average Sentiment")
    ax1.set_title("Sentiment Trend Over Time")
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Sentiment Score")
    ax1.legend()
    st.pyplot(fig1)

# Step 4: Fetch classes for the selected department
if selected_department:
    query_classes = f"""
    SELECT DISTINCT 
        c.COURSE_CODE AS course_code
    FROM FACT_REVIEW AS r
    JOIN DIM_CLASS AS c ON r.CLASS_ID = c.CLASS_ID
    JOIN DIM_DEPARTMENT AS d ON r.DEPARTMENT_ID = d.DEPARTMENT_ID
    WHERE d.DEPARTMENT_NAME = '{selected_department}'
    ORDER BY course_code;
    """
    df_classes = get_data_from_snowflake(query_classes)
    selected_class = st.selectbox(f"Select a Class in {selected_department}", df_classes['COURSE_CODE'])

    # Fetch class-specific data
    if selected_class:
        # Top professors for the class
        query_top_professors_class = f"""
        SELECT 
            p.professor_name AS professor_name, 
            AVG(r.QUALITY) AS avg_quality, 
            AVG(r.DIFFICULTY) AS avg_difficulty, 
            AVG(r.SENTIMENT_SCORE) AS avg_sentiment,
            COUNT(r.REVIEW_ID) AS review_count
        FROM FACT_REVIEW AS r
        JOIN DIM_CLASS AS c ON r.CLASS_ID = c.CLASS_ID
        JOIN DIM_PROFESSOR AS p ON r.PROFESSOR_ID = p.PROFESSOR_ID
        WHERE c.COURSE_CODE = '{selected_class}'
        GROUP BY professor_name
        ORDER BY avg_quality DESC, review_count DESC;
        """
        df_top_professors_class = get_data_from_snowflake(query_top_professors_class)

        # Trend of metrics over time
        query_metrics_trend = f"""
        SELECT 
            YEAR(r.DATE) AS year, 
            AVG(r.QUALITY) AS avg_quality, 
            AVG(r.DIFFICULTY) AS avg_difficulty
        FROM FACT_REVIEW AS r
        JOIN DIM_CLASS AS c ON r.CLASS_ID = c.CLASS_ID
        WHERE c.COURSE_CODE = '{selected_class}'
        GROUP BY year
        ORDER BY year;
        """
        df_metrics_trend = get_data_from_snowflake(query_metrics_trend)


        query_sentiment_trend = f"""
        SELECT 
            YEAR(r.DATE) AS year, 
            AVG(r.SENTIMENT_SCORE) AS avg_sentiment
        FROM FACT_REVIEW AS r
        JOIN DIM_CLASS AS c ON r.CLASS_ID = c.CLASS_ID
        WHERE c.COURSE_CODE = '{selected_class}'
        GROUP BY year
        ORDER BY year
        """
        df_sentiment_trend = get_data_from_snowflake(query_sentiment_trend)

        # Display class-level data
        st.subheader(f"Top Professors for {selected_class}")
        st.write(df_top_professors_class)

        # Trend of metrics plot
        fig2, ax2 = plt.subplots()
        ax2.plot(df_metrics_trend['YEAR'], df_metrics_trend['AVG_QUALITY'], label="Quality", color="blue")
        ax2.plot(df_metrics_trend['YEAR'], df_metrics_trend['AVG_DIFFICULTY'], label="Difficulty", color="orange")
        ax2.set_title(f"Metrics Trend for {selected_class}")
        ax2.set_xlabel("Year")
        ax2.set_ylabel("Score")
        ax2.legend()
        st.pyplot(fig2)

        # Sentiment trend plot
        fig3, ax3 = plt.subplots()
        ax3.plot(df_sentiment_trend['YEAR'], df_sentiment_trend['AVG_SENTIMENT'], label="Sentiment Over Time")
        ax3.axhline(df_sentiment_avg['AVG_SENTIMENT'][0], color="red", linestyle="--", label="Average Sentiment")
        ax3.set_title("Sentiment Trend Over Time")
        ax3.set_xlabel("Year")
        ax3.set_ylabel("Sentiment Score")
        ax3.legend()
        st.pyplot(fig3)

        # Grade distribution query
        query_grade_distribution_class = f"""
        SELECT 
            r.GRADE AS grade, 
            COUNT(*) AS count
        FROM FACT_REVIEW AS r
        JOIN DIM_CLASS AS c ON r.CLASS_ID = c.CLASS_ID
        WHERE c.COURSE_CODE = '{selected_class}'
        GROUP BY grade
        ORDER BY count DESC;
        """
        df_grade_distribution_class = get_data_from_snowflake(query_grade_distribution_class)

        # Grade distribution pie chart
        df_grade_distribution_class['PERCENTAGE'] = df_grade_distribution_class['COUNT'] / df_grade_distribution_class['COUNT'].sum() * 100
        df_grade_distribution_class['GRADE'] = np.where(df_grade_distribution_class['PERCENTAGE'] < 1.0, 'Other', df_grade_distribution_class['GRADE'])
        df_grade_distribution_class_grouped = df_grade_distribution_class.groupby('GRADE', as_index=False).agg({'COUNT': 'sum'})

        st.subheader(f"Grade Distribution for {selected_class}")
        fig3, ax3 = plt.subplots()
        ax3.pie(
            df_grade_distribution_class_grouped['COUNT'], 
            labels=df_grade_distribution_class_grouped['GRADE'], 
            autopct='%1.1f%%',
            textprops={'fontsize': 10}
        )
        ax3.set_title(f"Grade Distribution for {selected_class}")
        st.pyplot(fig3)

