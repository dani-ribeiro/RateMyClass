# Rate My Class
Introducing a new way to choose courses! Rate My Class allows students to explore professor quality, course difficulty, and overall sentiment across departments, courses, and professors, aiding in course selection and academic planning. The data pipeline runs daily to ensure up-to-date reviews. Developed using Python, Apache Airflow, Snowflake, TypeScript, and GraphQL.

## Installation
1. Clone the repository
2. Install the required Python packages:
```bash
pip install -r requirements.txt
```
3. Install the required npm packages
```bash
npm install --prefix pipeline/dags/data_collection/get_professors
```
## Usage
### Data Pipeline
1. Initialize the Airflow project
    ```bash
    airflowctl init pipeline/
    ```
    You may be warned that the directory is not empty: type 'Y' or agree. This is OK<br><br> 
2. Build the Airflow pipeline
    ```bash
    airflowctl build pipeline/
    ```
3. Start the Airflow pipeline
    ```bash
    airflowctl start pipeline/
    ```
4. Open `localhost:8080` in your browser using the generated password from [pipeline/standalone_admin_password.txt](pipeline/standalone_admin_password.txt)
5. Navigate to `http://localhost:8080/connection/list/` and create a new Snowflake connection with your account credentials.
    ```
    Connection ID: snowflake_default    // DO NOT CHANGE
    Connection Type: Snowflake          // DO NOT CHANGE
    Schema: RATE_MY_CLASS               // DO NOT CHANGE
    Login: ANIMAL
    Password: SNOWFLAKE-PASSWORD
    Extra: {
            "account": "SNOWFLAKE-ACCOUNT",
            "warehouse": "ANIMAL_WH",
            "database": "ANIMAL_DB",
            "schema": "RATE_MY_CLASS",  // DO NOT CHANGE
            "role": "TRAINING_ROLE",    // DO NOT CHANGE
            "insecure_mode": false      // DO NOT CHANGE
            }
    Account: SNOWFLAKE-ACCOUNT
    Warehouse: ANIMAL_WH
    Database: ANIMAL_DB
    Role: TRAINING_ROLE                 // DO NOT CHANGE
    ```
6. Unpause and trigger the `pipeline` DAG
7. Watch the magic happen

    __NOTE__:<br>
    The `get_reviews` task in the `data_collection` task group may take around 10 minutes to complete. This is normal as it's requesting every professor review at WashU from RateMyProfessors. If [pipeline/dags/reviews.csv](pipeline/dags/reviews.csv) is already saved, you can skip the `get_reviews` task by commenting it out, and uncommenting the dummy operator for `get_reviews`. Lines 67-77 in [pipeline.py](pipeline/dags/pipeline.py)

    ```python
    # task: get_reviews
    # gets all reviews for every professor at WashU and writes to data_cleaning/reviews.csv
        get_reviews = PythonOperator(
            task_id='get_reviews',
            python_callable=get_reviews
        )

    # use this to skip get_reviews task
    #get_reviews = DummyOperator(
    #    task_id='get_reviews'
    #)
    ```
8. Once the data pipeline has successfully completed, all of the data should appear in your Snowflake account!

### Data Visualization
9. Connect your Snowflake account in [streamlit_app/snowflake_info.py](streamlit_app/snowflake_info.py)
    ```python
    class SnowflakeInfo:
        ANIMAL = 'ANIMAL'
        USERNAME = 'ANIMAL'
        PASSWORD = 'SNOWFLAKE-PASSWORD'
        ACCOUNT = 'SNOWFLAKE-ACCOUNT'
        DATABASE = f'{ANIMAL}_DB'   // DO NOT CHANGE   
        SCHEMA = 'RATE_MY_CLASS'    // DO NOT CHANGE
    ```
10. Run data visualizations
    ```bash
    streamlit run streamlit_app/rmc_app.py 
    ```
11. Open `localhost:8501` in your browser

12. Explore!


## Data Collection: Credits
The data collection process consists of two main steps: professor information, and review retrieval. We first get a list of all professors at WashU using the open-source @ritchiefu/rate-my-professors GraphQL API wrapper (linked below), outputting the data directly to `professors.json` in the `get_reviews` directory. Next, the `get_reviews.py` script reads this file and gets all reviews for each professor, storing them in `reviews.csv`.

GraphQL API Wrapper: @ritchiefu/rate-my-professors: https://www.npmjs.com/package/@ritchiefu/rate-my-professors


## Data Preview: Washington University in St. Louis
reviews.csv is too large to upload to this repository: https://drive.google.com/file/d/10TunDSxvyIv831bvqMqObv6l5OqFTakf/view?usp=drive_link