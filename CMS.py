import psycopg2
import pandas as pd

l1 = []
columns=("Id", "Author", "Published_On", "Blog_Text", "Created_On")

host = "lkpgsql001.postgres.database.azure.com"
dbname = "firstdb"
user = "pdadmin@lkpgsql001"
password = "Azure$2022"
sslmode  = "require"
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
try:
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM CMS.BLOGS')

    print(columns)
    for record in cursor:
        l1.append(record)
        print(record)

    for i in l1:
        tempdf = pd.DataFrame(columns=columns)
        tempdf.loc[0] = i
        # Change the below path accordingly to the environment to export the output.
        path = r'C:\Users\Ravikumar\Desktop\CMS\\'+tempdf['Author'][0]+'-'+str(tempdf['Created_On'][0].strftime("%B"))
        if i[2] == None:
            path+='-Draft-blogs.csv'
        else:
            path+='-Published-blogs.csv'
        tempdf.to_csv(path,index=False)
    
    cursor.close()
    conn.close()


except Exception as e:
    print(e)
