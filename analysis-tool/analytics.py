import streamlit as st
import psycopg2
import pandas as pd

@st.cache(show_spinner=False)
def _load_data(filepath):
    return pd.read_csv(filepath)


# Database connection
con = None
try:
    con = psycopg2.connect(database='dbc', user='postgres',
                           password='postgres')
    cur = con.cursor()
    st.write("""
            # Business Leads
        """)
    st.text('List')
    with st.beta_expander('Prospects', expanded=True):
        how_to_load = st.selectbox('Countries List', ('Canada', 'USA'))
        if how_to_load == 'Canada':
#            df = pd.read_sql_query("SELECT * FROM canada_01_raw WHERE first = 'Kyle'", con)
            df = pd.read_sql("SELECT * FROM canada_01_raw", con)
#            df.drop(['unknown', 'location2','time1', 'time2'],axis = 1, inplace = True)
            df[['city1', 'state1']] = df.location1.str.split(",",expand=True)
            stateList = df.state1.unique()
            states = st.selectbox('States', stateList)
            for state in stateList:
                if states == state:
                    df = df[df.state1.isin([state])]

#            df.drop(['unknown','location1', 'location2','time1', 'time2'],axis = 1, inplace = True)
        if how_to_load == 'USA':
            df = pd.read_sql_query("SELECT * FROM usa_01_raw WHERE first = 'Kyle'", con)
            #df = pd.read_sql_query('SELECT * FROM usa_01_raw', con)
            # Not working
            #st.write(df.head(10))
            df[['city1', 'state1']] = df.location1.str.split(",",expand=True)
            stateList = df.state1.unique()
            states = st.selectbox('States', stateList)
            for state in stateList:
                if states == state:
                    df = df[df.state1.isin([state])]

            df.drop(['unknown','location1', 'location2','time1', 'time2'],axis = 1, inplace = True)
            #df['city1', 'state1'] = df.location1.str.split(",", expand=True)
    if df is not None:
        with st.beta_expander('Data Preview', expanded=True):
            with st.spinner('Loading Data...'):
                st.dataframe(df)
                st.write('`{}` rows, `{}` columns'.format(df.shape[0],df.shape[1]))

except psycopg2.DatabaseError as e:
    print(f'Error {e}')
    sys.exit(1)

finally:
    if con:
        con.close()