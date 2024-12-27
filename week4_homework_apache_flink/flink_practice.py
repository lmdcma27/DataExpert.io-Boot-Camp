from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit,col
from pyflink.table import  expressions as expr
from pyflink.table.window import Tumble




#take info from kafka producer
def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

#create table of logs in postgres
def create_sessionizes_by_ip_host(t_env):
    table_name = 'sessionizes_by_ip_host'
    sink_ddl = f"""
        CREATE TABLE {table_name} (               
            ip VARCHAR,
            host VARCHAR,
            event_hour TIMESTAMP(3),
            num_logs BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_averge_user_event_by_host(t_env):
    table_name = 'averge_user_event_by_host'  
    sink_ddl = f"""
        CREATE TABLE {table_name} (                        
            ip VARCHAR,
            host VARCHAR,            
            avg_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """  
    t_env.execute_sql(sink_ddl)
    return table_name


def create_avg_events_by_user_host(t_env):
    table_name = 'averge_user_event_by_host'  
    sink_ddl = f"""
        CREATE TABLE {table_name} (                           
            ip VARCHAR,
            host VARCHAR,            
            avg_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """  
    #t_env.execute_sql(sink_ddl)
    return table_name

def create_avg_events_by_host(t_env):
    table_name = 'avg_events_by_host'  
    sink_ddl = f"""
        CREATE TABLE {table_name} (            
            host VARCHAR,            
            avg_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """  
    #t_env.execute_sql(sink_ddl)
    return table_name


def analyze_sessions(t_env):    

    # Create the sessionizes_by_ip_host table (assuming it's already defined)
    create_sessionizes_by_ip_host(t_env)

    # Create target tables if they don't exist
    t_env.execute_sql(f"""
        CREATE TABLE avg_events_by_user_host (
            ip VARCHAR,
            host VARCHAR,
            avg_events BIGINT,
            PRIMARY KEY (ip, host) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = 'avg_events_by_user_host',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE avg_events_by_host (
            host VARCHAR,
            avg_events BIGINT,
            PRIMARY KEY (host) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = 'avg_events_by_host',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Query to calculate average events per session for Tech Creator
    avg_events_query = """
        SELECT ip, host, AVG(num_logs) AS avg_events
        FROM sessionizes_by_ip_host  
        WHERE host LIKE '%techcreator%'      
        GROUP BY ip, host
    """
    avg_events_table = t_env.sql_query(avg_events_query)

    # Insert the result into the target table (avg_events_by_user_host)
    avg_events_table.execute_insert("avg_events_by_user_host")

    # Compare results between different hosts
    compare_hosts_query = """
        SELECT host, AVG(num_logs) AS avg_events
        FROM sessionizes_by_ip_host        
        GROUP BY host
    """
    compare_hosts_table = t_env.sql_query(compare_hosts_query)
    
    # Insert the result into the target table (avg_events_by_host)
    compare_hosts_table.execute_insert("avg_events_by_host")



#handle location
class GetLocation(ScalarFunction):
  def eval(self, ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': os.environ.get("IP_CODING_KEY")
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return json.dumps({})

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')
    return json.dumps({'country': country, 'state': state, 'city': city})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 10000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)    
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        
        sessionizes_by_ip_host = create_sessionizes_by_ip_host(t_env)
                
        t_env.from_path(source_table)\
            .window(
            Tumble.over(lit(5).minutes).on(col("window_timestamp")).alias("w")
        ).group_by(            
            col("w"),
            col("ip"),
            col("host")
        ) \
            .select(
                    col("ip"),
                    col("host"),
                    col("w").start.alias("event_hour"),                                
                    lit(1).count.alias("num_logs")
            ) \
            .execute_insert(sessionizes_by_ip_host).wait()
                

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

def log_aggregation2():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 10000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings) 
    try: 

        analyze_sessions(t_env)

    except Exception as e:
        print("Writing records from JDBC to JDBC failed:", str(e)) 

def log_aggregation3():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 10000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings) 
    try:                
        avg_events_by_user_host = create_avg_events_by_user_host(t_env)
        avg_events_by_host = create_avg_events_by_host(t_env)        
        sessionizes_by_ip_host = create_sessionizes_by_ip_host(t_env)

        # Average number of web events of a session from a user on Tech Creator
        t_env.from_path(sessionizes_by_ip_host) \
            .filter(col("host").like("%techcreator%")) \
            .group_by(                        
                col("ip"),
                col("host")
            ) \
            .select(
                col("ip"),
                col("host"),                    
                col("num_logs").average.alias("avg_events")
            ) \
            .execute_insert(avg_events_by_user_host).wait()

        # Compare results between different hosts 
        t_env.from_path(sessionizes_by_ip_host) \
            .group_by(                                    
                col("host")                          
            ) \
            .select(                    
                col("host"),                    
                col("num_logs").average.alias("avg_events")
            ) \
            .execute_insert(avg_events_by_host).wait()

    except Exception as e:
        print("Writing records from JDBC to JDBC failed:", str(e))


if __name__ == '__main__':
    #homework parte 1&2
    #log_aggregation()

    #homework parte 3.a&3.b
    log_aggregation2() #option1
    #log_aggregation3() #option2