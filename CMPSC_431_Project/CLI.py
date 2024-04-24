import DatabaseAPI
import psycopg2
import json
with open("mappings.json","r") as file:
        mappings = json.load(file)
dbname  = mappings["database_keys"]["dbname"]
user  = mappings["database_keys"]["user"]
password  = mappings["database_keys"]["password"]
conn = DatabaseAPI.connect_db(dbname,user,password)

def insert_data():

    try:
        table = str(input("Enter Table Name: "))
        columns = str(input("Enter Columns "))
        values = str(input("Enter Values: "))

        column_list = columns.split(',')
        value_list = values.split(',')
        insert_query = f"INSERT INTO public.\"{table}\" ({', '.join(column_list)}) VALUES ({', '.join(['%s']*len(value_list))})"
        
        
        cur = conn.cursor()
        cur.execute(insert_query, value_list)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def delete_data():
    
    try:
        table = str(input("Enter Table Name: "))
        condition = str(input("Enter Condition "))

        delete_query = f"DELETE FROM public.\"{table}\" WHERE {condition}"
        cur = conn.cursor()
        cur.execute(delete_query)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def update_data():
    
    try:
        table = str(input("Enter Table Name: "))
        Value = str(input("Enter Value "))
        condition = str(input("Enter Condition "))

        update_query = f"UPDATE public.\"{table}\" SET {Value} WHERE {condition}"
        cur = conn.cursor()
        cur.execute(update_query)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def search_data():
        
    try:
        table = str(input("Enter Table Name: "))
        columns = str((input("Enter Columns ")))
        condition = str(input("Enter Condition "))

        select_query = f"SELECT {columns} FROM public.\"{table}\" WHERE {condition}"
        cur = conn.cursor()
        print(select_query)
        cur.execute(select_query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def aggregate_functions():
        
    try:
        table = str(input("Enter Table Name: "))
        columns = str((input("Enter Columns ")))
        agg_function = str(input("Enter Aggregate function "))
        
        cur = conn.cursor()
        select_query = f"SELECT {agg_function}({columns}) FROM  public.\"{table}\""       
        cur.execute(select_query)
        result = cur.fetchone()[0]
        print(result)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def sort_data():

    try:
        table = str(input("Enter Table Name: "))
        columns = str((input("Enter Columns ")))
        value  = str(input("Enter Order Value "))
        order =  str(input("Enter Order "))
        
        cur = conn.cursor()
        sort_query = f"SELECT {columns} FROM public.\"{table}\" ORDER BY {value} {order}"
        cur.execute(sort_query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)    
    
def join_data():
        
    try:
        table1 = str(input("Enter Table 1 Name: "))
        table2 = str(input("Enter Table 2 Name: "))
        columns = str((input("Enter Columns ")))
        condition  = str(input("Enter Condition "))
        cur = conn.cursor()

        join_query = f"SELECT {columns} FROM public.\"{table1}\" INNER JOIN public.\"{table2}\" ON {condition}"
        print(join_query)
        cur.execute(join_query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def group_data():
    
    try:    
        table = str(input("Enter Table Name: "))
        
        column = str((input("Enter Columns ")))
        
        cur = conn.cursor()
        group_query = f"SELECT {column}, COUNT(*) FROM public.\"{table}\" GROUP BY {column}"
        
        cur.execute(group_query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def subqueries_data():
    
    try:
        print("Please select an option:")
        print("1. Injured Players with Team Perfoming Below Average")
        print("2. Injured Players with Team Perfoming Above Average")
        
        option = int(input("Enter your choice: "))
        cur = conn.cursor()
        if option == 1:
            group_query = f"SELECT Player FROM public.\"Injury\" WHERE Team_id IN ((SELECT Team_id FROM public.\"Western_conference\" where W <= (SELECT AVG(W) from public.\"Western_conference\")) Union (SELECT Team_id FROM public.\"Eastern_conference\" where W <= (SELECT AVG(W) from public.\"Eastern_conference\")) )"
        if option == 2:
            group_query = f"SELECT Player FROM public.\"Injury\" WHERE Team_id IN ((SELECT Team_id FROM public.\"Western_conference\" where W > (SELECT AVG(W) from public.\"Western_conference\")) Union (SELECT Team_id FROM public.\"Eastern_conference\" where W > (SELECT AVG(W) from public.\"Eastern_conference\")) )"
        
        cur.execute(group_query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
def transaction_data():
        
    
    print("Please select an option:")
    print("1. Register a Team")
    print("2. Register a Player")
    option = int(input("Enter your choice: "))
    cur = conn.cursor()
    if option == 1:
        
        team = str(input("Enter Team's Name: "))
        coach = str(input("Enter Coach's Name: "))
        stadium = str(input("Enter Stadium's Name: "))
        team = f"'{team}'"
        coach = f"'{coach}'"
        stadium = f"'{stadium}'"
        transaction_query = f'''
BEGIN;
DO $$ 
DECLARE 
    team_exists INTEGER;
    coach_exists INTEGER;
    stadium_exists INTEGER;
    team_id_var INTEGER;
BEGIN
    SELECT COUNT(*) INTO team_exists FROM public."Teams" WHERE team = {team};

    IF team_exists > 0 THEN
        RAISE EXCEPTION 'Registration failed: Team already exists.';
    ELSE
        INSERT INTO public."Teams" (team) VALUES ({team}) RETURNING team_id INTO team_id_var;

        SELECT COUNT(*) INTO coach_exists FROM public."Coaches" WHERE coach = {coach} AND team_id IS NOT NULL;

        IF coach_exists > 0 THEN
            RAISE EXCEPTION 'Registration failed: Coach already coaching another team.';
        ELSE
            INSERT INTO public."Coaches" (coach, team_id) VALUES ({coach}, team_id_var);

            SELECT COUNT(*) INTO stadium_exists FROM public."Stadiums" WHERE arena = {stadium} AND team_id IS NOT NULL;

            IF stadium_exists > 0 THEN
                RAISE EXCEPTION 'Registration failed: Stadium already being used by another team.';
            ELSE
                INSERT INTO public."Stadiums" (arena, team_id) VALUES ({stadium},team_id_var);
            END IF;
        END IF;
    END IF;
END $$;
COMMIT;
'''
    
        
    cur.execute(transaction_query)
    conn.commit()
   
        


def display_menu():

    print("\nWelcome to the Database CLI Interface!\n\n")

    print("Please select an option:")
    print("1. Insert Data")
    print("2. Delete Data")
    print("3. Update Data")
    print("4. Search Data")
    print("5. Aggregate Functions")
    print("6. Sorting")
    print("7. Joins")
    print("8. Grouping")
    print("9. Subqueries")
    print("10. Transactions")
    print("12. Exit")


def main():

    display_menu()
    
    while True:
        
        try:
            option = int(input("Enter your choice: "))
            if option == 12:
                print("Exiting the script.")
                break
            elif option == 1:
                insert_data()
            elif option == 2:
                delete_data()
            elif option == 3:
                update_data()
            elif option == 4:
                search_data()
            elif option == 5:
                aggregate_functions()
            elif option == 6:
                sort_data()
            elif option == 7:
                join_data()
            elif option == 8:
                group_data()
            elif option == 9:
                subqueries_data()
            elif option == 10:
                transaction_data()
            else:
                print("Invalid option. Please select a valid option.")
        except ValueError:
            print("Invalid input. Please enter a number.")

if __name__ == "__main__":
    main()