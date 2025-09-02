import streamlit as st
import pymysql
import pandas as pd
import plotly.express as px
import requests 
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import squarify

st.set_page_config(layout="wide")      
st.title('PhonePe transaction insights')

# Database connection
def get_connection():
    return pymysql.connect(
    host="localhost",
    user="root",
    password="root123",
    database='phonepe_insurance'
)

# Helper function to fetch data
def fetch_data(query,params=None):
    conn = get_connection()
    with conn.cursor() as cursor : 
        cursor.execute(query,params or ())
        result = cursor.fetchall()
    conn.close()
    return result

# Sidebar
main=st.sidebar.radio("Navigation",['Explore Data','Reports'])

# Each class for each pages
class Transaction:
    def get_transaction_data(state,year,quarter):
        query= """
            SELECT State, Year, Quater, Transaction_name, 
                SUM(Transaction_amount) AS Total_amount
            FROM agg_transaction
            WHERE State=%s AND Year=%s AND Quater=%s
            GROUP BY State, Year, Quater, Transaction_name; 
        """
        return fetch_data(query,(state,year,quarter))

    def get_map_transaction_data(year,quarter):
        query = """
        select State,Year,Quater,sum(Transaction_count),sum(Transaction_amount) from map_transactions_district where Year = %s and Quater = %s group by State,Year,Quater; 
        """
        return fetch_data(query,(year,quarter))

    def get_top_transaction_data(state,year,quarter):
        query = """select State, Year,Quater,District_name, sum(District_Transaction_amount) as total_amount 
        from top_transactions_district
        where State = %s and Year = %s and Quater = %s
        group by State,Year,Quater, District_name; 
        """
        return fetch_data(query,(state,year,quarter))

class Insurance:
    def get_insurance_data(state,year,quarter):
        query= """
            SELECT State, Year, Quater, Transaction_name, 
                SUM(Transaction_amount) AS Total_amount
            FROM insurance
            WHERE State=%s AND Year=%s AND Quater=%s
            GROUP BY State, Year, Quater, Transaction_name; 
        """
        return fetch_data(query,(state,year,quarter))
    
    def get_map_insurance_data(year,quarter):
        query = """
        select State,Year,Quater,Transacion_type,sum(count),sum(Transacion_amount) from map_insurance_data where Year = %s and Quater = %s group by State,Year,Quater,Transacion_type; 
        """
        return fetch_data(query,(year,quarter))

    def get_top_insurance_data(state,year,quarter):
        query = """select State, Year,Quater,District_name, District_Insurance_count,District_Insurance_amount as total_amount 
        from top_insurance_district
        where State = %s and Year = %s and Quater = %s; 
        """
        return fetch_data(query,(state,year,quarter))

class User:
    def get_user_data(State,year,quarter):
        query = """select State,Year,Quater,District_name,sum(District_Registered_Users) from top_users_district where State=%s and Year = %s and Quater = %s group by State,Year,Quater,District_name;"""
        return fetch_data(query,(State,year,quarter)) 
    def get_map_user_data(year,quarter):
        query = """select State,Year,Quater,sum(Registered_users),sum(Count) from users where Year = %s and Quater = %s group by State,Year,Quater;"""
        return fetch_data(query,(year,quarter)) 
    
    def get_total_user_data(State,year,quarter):
        query = """select State,Year,Quater,Brand ,sum(Registered_users),sum(Count) from users where State=%s and Year = %s and Quater = %s group by State,Year,Quater,Brand;"""
        return fetch_data(query,(State,year,quarter))
        
# map state names class
class map:
    state_name_map = {
        'andaman-&-nicobar-islands': 'Andaman & Nicobar',
        'andhra-pradesh': 'Andhra Pradesh',
        'arunachal-pradesh': 'Arunachal Pradesh',
        'assam': 'Assam',
        'bihar': 'Bihar',
        'chandigarh': 'Chandigarh',
        'chhattisgarh': 'Chhattisgarh',
        'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
        'delhi': 'Delhi',
        'goa': 'Goa',
        'gujarat': 'Gujarat',
        'haryana': 'Haryana',
        'himachal-pradesh': 'Himachal Pradesh',
        'jammu-&-kashmir': 'Jammu & Kashmir',
        'jharkhand': 'Jharkhand',
        'karnataka': 'Karnataka',
        'kerala': 'Kerala',
        'ladakh': 'Ladakh',
        'lakshadweep': 'Lakshadweep',
        'madhya-pradesh': 'Madhya Pradesh',
        'maharashtra': 'Maharashtra',
        'manipur': 'Manipur',
        'meghalaya': 'Meghalaya',
        'mizoram': 'Mizoram',
        'nagaland': 'Nagaland',
        'odisha': 'Odisha',
        'puducherry': 'Puducherry',
        'punjab': 'Punjab',
        'rajasthan': 'Rajasthan',
        'sikkim': 'Sikkim',
        'tamil-nadu': 'Tamil Nadu',
        'telangana': 'Telangana',
        'tripura': 'Tripura',
        'uttar-pradesh': 'Uttar Pradesh',
        'uttarakhand': 'Uttarakhand',
        'west-bengal': "West Bengal"
    }

map_state = map.state_name_map

# DataFrames
class DataFrames:
    def transaction_df(select_state,select_year,select_quarter):
        map_transaction = pd.DataFrame(Transaction.get_map_transaction_data(select_year,select_quarter),columns=['State','Year','Quater','Count','Amount'])
        map_transaction['State'] = map_transaction['State'].map(map_state) 
        
        agg_transaction = pd.DataFrame(Transaction.get_transaction_data(select_state,select_year,select_quarter),columns=['State', 'Year', 'Quarter', 'Transaction_name', 'Total_amount'])

        top_transaction = pd.DataFrame(Transaction.get_top_transaction_data(select_state,select_year,select_quarter),columns=['State','Year','Quarter','District Name','Total_Amount'])

        return map_transaction,agg_transaction,top_transaction

    def insurance_df(select_state,select_year,select_quarter):
        map_insurance = pd.DataFrame(Insurance.get_map_insurance_data(select_year,select_quarter),columns=['State','Year','Quater','Insurance_Type','Count','Amount'])
        map_insurance['State'] = map_insurance['State'].map(map_state)

        agg_insurance =  pd.DataFrame(Insurance.get_insurance_data(select_state,select_year,select_quarter),columns=['State', 'Year', 'Quarter', 'Insurance_name', 'Total_amount'])

        top_insurance = pd.DataFrame(Insurance.get_top_insurance_data(select_state,select_year,select_quarter),columns=['State','Year','Quarter','District Name','Count','Amount'])

        return map_insurance,agg_insurance,top_insurance

    def User_df(select_state,select_year,select_quarter):
        map_User = pd.DataFrame(User.get_map_user_data(select_year,select_quarter),columns=['State','Year','Quater','Registered Users','Count'])
        map_User['State'] = map_User['State'].map(map_state)

        agg_User =  pd.DataFrame(User.get_user_data(select_state,select_year,select_quarter),columns=['State', 'Year', 'Quarter', 'District Name','Registered Users'])

        top_User = pd.DataFrame(User.get_total_user_data(select_state,select_year,select_quarter),columns=['State','Year','Quater','Brand','Registered Users','Count'])

        return map_User,agg_User,top_User

# India Map
def india_map(data_frame,metric):
    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geojson_data = requests.get(geojson_url).json() 
    
    fig = px.choropleth(
        data_frame,
        geojson=geojson_data,
        featureidkey='properties.ST_NM',
        locations='State',
        color= metric,
        color_continuous_scale='Reds',
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        coloraxis_showscale=False
    )
    st.plotly_chart(fig, use_container_width=True)

# Selectbox
def getFilters(tableName,state='Select Transactional State',year='Select Transactional Year',quarter='Select Transactional Quarter'):
    col1,col2,col3 = st.columns(3)

    with col1:
        states = [row[0] for row in fetch_data(f"SELECT DISTINCT State FROM {tableName};")]
        select_state = st.selectbox(
            state,
            states,
            index=None,
            placeholder='Select an option...'
        )
    
    with col2:
        years = [row[0] for row in fetch_data(f"SELECT DISTINCT Year FROM {tableName} ORDER BY Year;")]
        select_year = st.selectbox(year,years)
    
    with col3:
        quarters = [row[0] for row in fetch_data(f"SELECT DISTINCT Quater FROM {tableName} WHERE Year = {select_year} ORDER BY Quater;")]
        select_quarter = st.selectbox(quarter,quarters)

    return select_state,select_year,select_quarter


if main == 'Explore Data':
    tab1,tab2,tab3 = st.tabs(['Payments','Insurance','User'])

    with tab1:
        with st.container(border=True,height='content'):
            select_state,select_year,select_quarter = getFilters('agg_transaction')
            map_transaction,agg_transaction,top_transaction = DataFrames.transaction_df(select_state,select_year,select_quarter)

        with st.container(border=True,height='content'):
            st.subheader('Overall Transactional Data')
            st.dataframe(map_transaction,hide_index=True)
            india_map(map_transaction,map_transaction['Amount'])
        
        if select_state and select_year and select_quarter:
            with st.container(border=True,height="content"):
                st.subheader(f"{select_state.capitalize()} Transactional Data")
                st.dataframe(agg_transaction,hide_index=True)
            
                col1,col2 = st.columns(2)
                amount = agg_transaction['Total_amount']
                values = agg_transaction['Transaction_name']
                with col1: 
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.bar(values, amount)
                    ax.set_ylabel("Total Amount")
                    ax.set_title("Transaction Types")
                    plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                    st.pyplot(fig)

                with col2:
                    fig, ax = plt.subplots(figsize=(6,5))   # same height as bar chart
                    ax.pie(
                        amount,
                        labels=values,
                        startangle=140
                    )
                    ax.set_title("Transaction Distribution")
                    st.pyplot(fig)

            if select_state and select_year and select_quarter:
                st.subheader(f"{select_state.capitalize()} District Transactional data")
                st.dataframe(top_transaction,hide_index=True)
                
    with tab2:
        # Insurance section
        with st.container(border=True,height='content'):
            select_state,select_year,select_quarter = getFilters('map_insurance_data','Select Insurance State','Select Insurance Year','Select Insurance Quarter')
            map_insurance, agg_insurance, top_insurance = DataFrames.insurance_df(select_state,select_year,select_quarter)

        with st.container(border=True,height='content'):
            st.subheader('Overall Insurance Data')
            if select_year and select_quarter :
                st.dataframe(map_insurance,hide_index=True)

                india_map(map_insurance,map_insurance['Amount'])
        
        if select_state and select_year and select_quarter:
            with st.container(border=True,height="content"):
                st.subheader(f"{select_state.capitalize()} insurance Data")
                st.dataframe(agg_insurance,hide_index=True)
               
            if select_state and select_year and select_quarter:
                with st.container(border=True,height="content"):
                    st.subheader(f"{select_state.capitalize()} District insurance data")
                    st.dataframe(top_insurance,hide_index=True)
                    col1,col2 = st.columns(2)
                    with col1:
                        fig,ax = plt.subplots(figsize=(6,6))
                        ax.bar(top_insurance['District Name'],top_insurance['Amount'])
                        ax.set_ylabel('Amount')
                        ax.set_xlabel('District Name')
                        ax.set_title('Insurance Amount')
                        plt.xticks(rotation=30, ha='right', fontsize=6, wrap=True) 
                        st.pyplot(fig)
                    with col2:
                        fig,ax = plt.subplots(figsize=(6,6))
                        ax.pie(
                            top_insurance['Count'],
                            labels=top_insurance['District Name']
                        )
                        ax.set_title('Insurance Count')
                        st.pyplot(fig) 
    
    with tab3:
        with st.container(border=True,height='content'):
            select_state,select_year,select_quarter = getFilters('users','Select Users State','Select Users Year','Select Users Quarter')

            map_User, agg_User, top_User = DataFrames.User_df(select_state,select_year,select_quarter)
        
        with st.container(border=True,height='content'):
            st.subheader('Overall User Data')
            st.dataframe(map_User,hide_index=True)

            india_map(map_User,map_User['Count'])

            if select_state and select_year and select_quarter:
                with st.container(border=True,height="content"):
                    st.subheader(f"{select_state.capitalize()} users Data")
                    st.dataframe(agg_User,hide_index=True)
                    col1,col2 = st.columns(2)
                    amount = agg_User['Registered Users']
                    values = agg_User['District Name']
                    with col1: 
                        fig, ax = plt.subplots(figsize=(6,4))
                        ax.bar(values, amount)
                        ax.set_ylabel("Registered Users")
                        ax.set_xlabel("District Name")
                        plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                        st.pyplot(fig)

                    with col2:
                        fig, ax = plt.subplots(figsize=(6,5))   # same height as bar chart
                        ax.pie(
                            amount,
                            labels=values,
                            startangle=140
                        )
                        ax.set_title("Users Distribution")
                        st.pyplot(fig)

                with st.container(border=True,height="content"): 
                    st.dataframe(top_User,hide_index=True)

                    col1,col2 = st.columns(2)
                    brand = top_User['Brand']
                    values = top_User['Count']
                    with col1: 
                        fig, ax = plt.subplots(figsize=(6,4))
                        ax.bar(brand,values)
                        ax.set_ylabel("Registered Users")
                        ax.set_xlabel("Brand Name")
                        plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                        st.pyplot(fig)

                    with col2:
                        fig, ax = plt.subplots(figsize=(6,3))   
                        ax.pie(
                            values,
                            labels=brand,
                            startangle=140,
                            textprops={'fontsize': 5}
                        )
                        st.pyplot(fig)

if main == 'Reports':
    tab1,tab2,tab3 = st.tabs(['Payments','Insurance','User'])

    with tab1:
        with st.container(border=True):
            select_state,select_year,select_quarter = getFilters('agg_transaction')

        # Report 1
        with st.container(border=True):
            st.header('Decoding Transaction Dynamics on PhonePe')
            if select_state:
                set_state = select_state.replace('-',' ')

                # Overall trend
                st.subheader(f'{set_state.capitalize()} transactional trend over the years')
                query = """select State,Year,sum(District_Transaction_amount) as Transaction_amount from top_transactions_district where State = %s group by State, Year;"""
                df = pd.DataFrame(fetch_data(query,(select_state)),columns=['State','Year','Transaction Amount'])
                col1,col2 = st.columns(2)
                with col1 : 
                    fig, ax = plt.subplots(figsize=(6,5))
                    ax.plot(df['Year'],df['Transaction Amount'])
                    st.pyplot(fig)
                with col2 :
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.pie(
                        df['Transaction Amount'],
                        labels=df['Year']
                    )
                    st.pyplot(fig)

                # Minimum amount profit districts
                st.subheader(f'Top 10 lowest transactional zones in {set_state} during {select_year}')
                query = """select min(District_Transaction_amount) as Transaction_amount,District_name,Quater from top_transactions_district where State = %s and year = %s group by Quater,District_name order by Transaction_amount asc limit 10;"""
                df = pd.DataFrame(fetch_data(query,(select_state,select_year)),columns=['Transaction Amount','District','Quater'])
                df.insert(loc=0, column='Rank', value=np.arange(1,len(df)+1))

                st.dataframe(df,hide_index=True)
                col1,col2 = st.columns(2)
                with col1:
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.bar(df['District'],df['Transaction Amount'])
                    plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                    st.pyplot(fig)
                with col2:
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax = squarify.plot(sizes=df['Transaction Amount'], label=df['District'])
                    st.pyplot(fig)
        
        # Report 2 
        with st.container(border=True):
            st.header('Transaction Analysis across Districts')
            if select_state:
                st.subheader(f'Top 10 lowest transactional area zones in {set_state} during {select_year}')
                query = """select Pincode,Quater,min(Pincode_Transaction_count) as Transaction_count from top_transactions_pincode where State = %s and year = %s group by Quater,Pincode order by Transaction_count asc limit 10;"""
                df = pd.DataFrame(fetch_data(query,(select_state,select_year)),columns=['Pincode','Quarter','Transaction_count'])
                df.insert(loc=0, column='Rank', value=np.arange(1,len(df)+1))
                st.dataframe(df,hide_index=True)
                col1,col2 = st.columns(2)
                with col1:
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.bar(df['Pincode'],df['Transaction_count'])
                    plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                    st.pyplot(fig)
                with col2:
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax = squarify.plot(sizes=df['Transaction_count'], label=df['Pincode'])
                    st.pyplot(fig)

    with tab2:
        with st.container(border=True):
            select_state,select_year,select_quarter =  getFilters('map_insurance_data','Select Insurance State','Select Insurance Year','Select Insurance Quarter')
        # Report 1
        with st.container(border=True):
            st.header('Insurance Engagement Analysis')
            if select_year and select_quarter:
                st.subheader(f'Top 10 States with highest insurance amount in {select_year} at {select_quarter} quarter')
                query = """select State,Year,Quater,sum(District_Insurance_amount) as Total_amount from top_insurance_district where Year = %s and Quater =%s group by State,Year,Quater order by Total_amount desc limit 10;"""
                df = pd.DataFrame(fetch_data(query,(select_year,select_quarter)),columns=['State','Year','Quarter','Total amount'])
                st.dataframe(df,hide_index=True)
                col1,col2 = st.columns(2)
                with col1 : 
                    fig,ax = plt.subplots(figsize=(6,5))
                    ax.bar(df['State'],df['Total amount'])
                    plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                    st.pyplot(fig)
                with col2:
                    fig, ax = plt.subplots(figsize=(6,4))
                    size = df['Total amount'].astype('int')
                    ax = squarify.plot(sizes=size, label=df['State'])
                    st.pyplot(fig)

        # Report 2
        if select_state and select_year and select_quarter:
            with st.container(border=True):
                st.header("Insurance Transactional Analysis")
                query = """select State,Year,Quater,state_or_district_name,sum(Transacion_amount) as Total_amount from map_insurance_data where State=%s and Year = %s and Quater=%s group by State,Year,Quater,state_or_district_name order by Total_amount desc limit 10;"""
                df = pd.DataFrame(fetch_data(query,(select_state,select_year,select_quarter)),columns=['State','Year','Quarter','District Name','Total amount'])
                st.dataframe(df,hide_index=True)
                col1,col2 = st.columns(2)
                with col1 : 
                    fig,ax = plt.subplots(figsize=(6,5))
                    ax.bar(df['District Name'],df['Total amount'])
                    plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                    st.pyplot(fig)
                with col2:
                    fig, ax = plt.subplots(figsize=(6,4))
                    size = df['Total amount'].astype('int')
                    ax = squarify.plot(sizes=size, label=df['District Name'])
                    st.pyplot(fig)

    with tab3:
        with st.container(border=True):
            select_state,select_year,select_quarter = getFilters('users','Select Users State','Select Users Year','Select Users Quarter')
        # Report 1
        with st.container(border=True):
            st.header("Device Dominance and User Engagement Analysis")
            if select_state:
                set_state = select_state.replace('-',' ')
                st.subheader(f'{select_year} leading brands in {set_state} over the quarter {select_quarter}')
                query = """select State, Brand,sum(Count) as Count from users where State = %s and Year = %s and Quater = %s group by State, Brand order by Count limit 10;"""
                df = pd.DataFrame(fetch_data(query,(select_state,select_year,select_quarter)),columns=['State','Brand','Total Users'])
                st.dataframe(df,hide_index=True)
                col1,col2 = st.columns(2)
                with col1 : 
                    fig,ax = plt.subplots(figsize=(6,5))
                    ax.bar(df['Brand'],df['Total Users'])
                    plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                    st.pyplot(fig)
                with col2:
                    fig, ax = plt.subplots(figsize=(6,4))
                    size = df['Total Users'].astype('int')
                    ax = squarify.plot(sizes=size, label=df['Brand'])
                    st.pyplot(fig)
        # Report 2
        with st.container(border=True):
            st.header('User registeration analysis')
            if select_state and select_year:
                st.subheader(f'Registered Users analysis for {set_state} in {select_year}')
                query = """select State,Year,Quater, sum(Registered_users) from users where State=%s and Year = %s group by State,Year,Quater;"""
                df = pd.DataFrame(fetch_data(query,(select_state,select_year)),columns=['State','Year','Quater','Registered Users'])
                st.dataframe(df,hide_index=True)
                col1,col2 = st.columns(2)
                with col1 : 
                    fig,ax = plt.subplots(figsize=(6,5))
                    ax.plot(df['Quater'],df['Registered Users'])
                    plt.xticks(rotation=30, ha='right', fontsize=9, wrap=True) 
                    st.pyplot(fig)
                with col2:
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.pie(
                        df['Registered Users'],
                        labels=df['Quater']
                    )
                    st.pyplot(fig)





                



