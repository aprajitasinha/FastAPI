import psycopg2
import pandas as pd
import os

##fetch data from a postgresql database table using psycopg2
def connection():
    conn= psycopg2.connect(
        database="stockanalysis",
            user='aryanpatel',
            password="12345",
            host="localhost",
            port="5432"
    )
    return conn
def fetch_data(schema_name, intervl):
    try:
        conn=connection()
        conn.cursor()
        query=f'SELECT * FROM "{schema_name}"."candle_{intervl}"'
        df =pd.read_sql_query(query, conn)
        print(df)
        csv_file=df.to_csv(f'{schema_name}{interval}.csv',index=False)
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
        file_path = os.path.join(desktop_path, f"{schema_name}_{interval}.xlsx")

        # Save Excel file
        df.to_excel(file_path, index=False, engine='openpyxl')
        print(f"Excel file saved to: {file_path}")        
        conn.close()
        return df 
    except Exception as e:
        print(f"Error: {e}")
        conn.close()
        return None  
    
    
    
    
    
    
# def fetch_data(schema_name,interval):
#     try:
#         conn=connection()
#         cursor=conn.cursor()
#         query=f'SELECT * FROM "{schema_name}"."candle_{interval}"'
#         cursor.execute(query)
#         records=cursor.fetchall()
#         print(records)
#         df=pd.DataFrame(records,columns=['id','open','high','low','close','volume','epochtime'])
#         print(df)
#         csv_file=df.to_csv(f'{schema_name}.csv',index=False)
#         print(f"CSV file created: {csv_file}")
#         conn.close()
#         return records
#     except Exception as e:
#         print(f"Error: {e}")
#         conn.close()
#         return None
    


if __name__=="__main__":
    schema_name="UPL"
    interval="5m"
    fetch_data(schema_name,interval)