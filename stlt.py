import pandas as pd
import pymongo
import psycopg2
import plotly.express as px
import geopandas as gpd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
pd.set_option('display.max_columns', None)


def streamlit_config():

    # page configuration
    page_icon_url = 'https://github.com/iamsandeeprSand/Airbnb_Analysis/blob/main/airbnb.png'
    st.set_page_config(page_title='Airbnb',
                       page_icon=page_icon_url, layout="wide")

    # page header transparent color
    page_background_color = """
    <style>

    [data-testid="stHeader"] 
    {
    background: rgba(0,0,0,0);
    }

    </style>
    """
    st.markdown(page_background_color, unsafe_allow_html=True)

    # title and position
    st.markdown(f'<h1 style="text-align: center;">Airbnb Analysis</h1>',
                unsafe_allow_html=True)


class data_collection:
    sand = pymongo.MongoClient("mongodb+srv://iam_sandeep_r:rsandeep@cluster0.p8sldxv.mongodb.net/?retryWrites=true&w=majority")
    db = sand['sample_airbnb']
    col = db['listingsAndReviews']


class data_preprocessing:

    def primary():
        # direct feature columns
        data = []
        for i in data_collection.col.find({}, {'_id': 1, 'listing_url': 1, 'name': 1, 'property_type': 1, 'room_type': 1, 'bed_type': 1,
                                               'minimum_nights': 1, 'maximum_nights': 1, 'cancellation_policy': 1, 'accommodates': 1,
                                               'bedrooms': 1, 'beds': 1, 'number_of_reviews': 1, 'bathrooms': 1, 'price': 1,
                                               'cleaning_fee': 1, 'extra_people': 1, 'guests_included': 1, 'images.picture_url': 1,
                                               'review_scores.review_scores_rating': 1}):
            data.append(i)

        df_1 = pd.DataFrame(data)
        df_1['images'] = df_1['images'].apply(lambda x: x['picture_url'])
        df_1['review_scores'] = df_1['review_scores'].apply(
            lambda x: x.get('review_scores_rating', 0))

        # null value handling
        df_1['bedrooms'].fillna(0, inplace=True)
        df_1['beds'].fillna(0, inplace=True)
        df_1['bathrooms'].fillna(0, inplace=True)
        df_1['cleaning_fee'].fillna('Not Specified', inplace=True)

        # data types conversion
        df_1['minimum_nights'] = df_1['minimum_nights'].astype(int)
        df_1['maximum_nights'] = df_1['maximum_nights'].astype(int)
        df_1['bedrooms'] = df_1['bedrooms'].astype(int)
        df_1['beds'] = df_1['beds'].astype(int)
        df_1['bathrooms'] = df_1['bathrooms'].astype(str).astype(float)
        df_1['price'] = df_1['price'].astype(str).astype(float).astype(int)
        df_1['cleaning_fee'] = df_1['cleaning_fee'].apply(lambda x: int(
            float(str(x))) if x != 'Not Specified' else 'Not Specified')
        df_1['extra_people'] = df_1['extra_people'].astype(
            str).astype(float).astype(int)
        df_1['guests_included'] = df_1['guests_included'].astype(
            str).astype(int)

        return df_1

    def host():
        host = []
        for i in data_collection.col.find({}, {'_id': 1, 'host': 1}):
            host.append(i)

        df_host = pd.DataFrame(host)
        host_keys = list(df_host.iloc[0, 1].keys())
        host_keys.remove('host_about')

        # make nested dictionary to separate columns
        for i in host_keys:
            if i == 'host_response_time':
                df_host['host_response_time'] = df_host['host'].apply(
                    lambda x: x['host_response_time'] if 'host_response_time' in x else 'Not Specified')
            else:
                df_host[i] = df_host['host'].apply(
                    lambda x: x[i] if i in x and x[i] != '' else 'Not Specified')

        df_host.drop(columns=['host'], inplace=True)

        # data type conversion
        df_host['host_is_superhost'] = df_host['host_is_superhost'].map(
            {False: 'No', True: 'Yes'})
        df_host['host_has_profile_pic'] = df_host['host_has_profile_pic'].map(
            {False: 'No', True: 'Yes'})
        df_host['host_identity_verified'] = df_host['host_identity_verified'].map(
            {False: 'No', True: 'Yes'})

        return df_host

    def address():
        address = []
        for i in data_collection.col.find({}, {'_id': 1, 'address': 1}):
            address.append(i)

        df_address = pd.DataFrame(address)
        address_keys = list(df_address.iloc[0, 1].keys())

        # nested dicionary to separate columns
        for i in address_keys:
            if i == 'location':
                df_address['location_type'] = df_address['address'].apply(
                    lambda x: x['location']['type'])
                df_address['longitude'] = df_address['address'].apply(
                    lambda x: x['location']['coordinates'][0])
                df_address['latitude'] = df_address['address'].apply(
                    lambda x: x['location']['coordinates'][1])
                df_address['is_location_exact'] = df_address['address'].apply(
                    lambda x: x['location']['is_location_exact'])
            else:
                df_address[i] = df_address['address'].apply(
                    lambda x: x[i] if x[i] != '' else 'Not Specified')

        df_address.drop(columns=['address'], inplace=True)

        # bool data conversion to string
        df_address['is_location_exact'] = df_address['is_location_exact'].map(
            {False: 'No', True: 'Yes'})
        return df_address

    def availability():
        availability = []
        for i in data_collection.col.find({}, {'_id': 1, 'availability': 1}):
            availability.append(i)

        df_availability = pd.DataFrame(availability)
        availability_keys = list(df_availability.iloc[0, 1].keys())

        # nested dicionary to separate columns
        for i in availability_keys:
            df_availability['availability_30'] = df_availability['availability'].apply(
                lambda x: x['availability_30'])
            df_availability['availability_60'] = df_availability['availability'].apply(
                lambda x: x['availability_60'])
            df_availability['availability_90'] = df_availability['availability'].apply(
                lambda x: x['availability_90'])
            df_availability['availability_365'] = df_availability['availability'].apply(
                lambda x: x['availability_365'])

        df_availability.drop(columns=['availability'], inplace=True)
        return df_availability

    def amenities_sort(x):
        a = x
        a.sort(reverse=False)
        return a

    def amenities():
        amenities = []
        for i in data_collection.col.find({}, {'_id': 1, 'amenities': 1}):
            amenities.append(i)

        df_amenities = pd.DataFrame(amenities)

        # sort the list of amenities
        df_amenities['amenities'] = df_amenities['amenities'].apply(
            lambda x: data_preprocessing.amenities_sort(x))
        return df_amenities

    def merge_dataframe():
        df_1 = data_preprocessing.primary()
        df_host = data_preprocessing.host()
        df_address = data_preprocessing.address()
        df_availability = data_preprocessing.availability()
        df_amenities = data_preprocessing.amenities()

        df = pd.merge(df_1, df_host, on='_id')
        df = pd.merge(df, df_address, on='_id')
        df = pd.merge(df, df_availability, on='_id')
        df = pd.merge(df, df_amenities, on='_id')

        return df


class sql:

    def create_table():
        conn = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="sandeep",
                                port=5432,
                                database="PhonePe")
        sand = conn.cursor()
        sand.execute(f"""create table if not exists airbnb(
                            _id					varchar(255) primary key,
                            listing_url			text,
                            name				varchar(255),
                            property_type		varchar(255),
                            room_type			varchar(255),
                            bed_type			varchar(255),
                            minimum_nights		int,
                            maximum_nights		int,
                            cancellation_policy	varchar(255),
                            accommodates		int,
                            bedrooms			int,
                            beds				int,
                            number_of_reviews	int,
                            bathrooms			float,
                            price				int,
                            cleaning_fee		varchar(20),
                            extra_people		int,
                            guests_included		int,
                            images				text,
                            review_scores		int,
                            host_id				varchar(255),
                            host_url			text,
                            host_name			varchar(255),
                            host_location		varchar(255),
                            host_response_time			varchar(255),
                            host_thumbnail_url			text,
                            host_picture_url			text,
                            host_neighbourhood			varchar(255),
                            host_response_rate			varchar(255),
                            host_is_superhost			varchar(25),
                            host_has_profile_pic		varchar(25),
                            host_identity_verified		varchar(25),
                            host_listings_count			int,
                            host_total_listings_count	int,
                            host_verifications			text,
                            street				varchar(255),
                            suburb				varchar(255),
                            government_area		varchar(255),
                            market				varchar(255),
                            country				varchar(255),
                            country_code		varchar(255),
                            location_type		varchar(255),
                            longitude			float,
                            latitude			float,
                            is_location_exact	varchar(25),
                            availability_30		int,
                            availability_60		int,
                            availability_90		int,
                            availability_365	int,
                            amenities			text);""")

        conn.commit()
        conn.close()

    def data_migration():
        conn = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="sandeep",
                                port=5432,
                                database="PhonePe")
        sand = conn.cursor()
        df = data_preprocessing.merge_dataframe()

        sand.executemany("insert into airbnb \
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", df.values.tolist())
        conn.commit()
        conn.close()

    def delete_table():
        conn = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="sandeep",
                                port=5432,
                                database="PhonePe")
        sand = conn.cursor()
        sand.execute(f"""delete from airbnb;""")
        conn.commit()
        conn.close()


class plotly:

    def pie_chart(df, x, y, title, title_x=0.20):

        fig = px.pie(df, names=x, values=y, hole=0.5, title=title)

        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(text=df[y], textinfo='percent+value',
                          textposition='outside',
                          textfont=dict(color='white'))

        st.plotly_chart(fig, use_container_width=True)

    def horizontal_bar_chart(df, x, y, text, color, title, title_x=0.25):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)

        fig.update_layout(title_x=title_x, title_font_size=22)

        text_position = ['inside' if val >= max(
            df[x]) * 0.75 else 'outside' for val in df[x]]

        fig.update_traces(marker_color=color,
                          text=df[text],
                          textposition=text_position,
                          texttemplate='%{x}<br>%{text}',
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True)

    def vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)

        fig.update_layout(title_x=title_x, title_font_size=22)

        text_position = ['inside' if val >= max(
            df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color,
                          text=df[text],
                          textposition=text_position,
                          texttemplate='%{y}<br>%{text}',
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)

    def line_chart(df, x, y, text, textposition, color, title, title_x=0.25):

        fig = px.line(df, x=x, y=y, labels={
                      x: '', y: ''}, title=title, text=df[text])

        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(line=dict(color=color, width=3.5),
                          marker=dict(symbol='diamond', size=10),
                          texttemplate='%{x}<br>%{text}',
                          textfont=dict(size=13.5),
                          textposition=textposition,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)


class feature:

    def feature(column_name, order='count desc', limit=10):
        conn = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="sandeep",
                                port=5432,
                                database="PhonePe")
        sand = conn.cursor()
        sand.execute(f"""select distinct {column_name}, count({column_name}) as count
                           from airbnb
                           group by {column_name}
                           order by {order}
                           limit {limit};""")
        conn.commit()
        s = sand.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=[column_name, 'count'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['percentage'] = data['count'].apply(
            lambda x: str('{:.2f}'.format(x/55.55)) + '%')
        data['y'] = data[column_name].apply(lambda x: str(x)+'`')
        return data

    def cleaning_fee():
        conn = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="sandeep",
                                port=5432,
                                database="PhonePe")
        sand = conn.cursor()
        sand.execute(f"""select distinct cleaning_fee, count(cleaning_fee) as count
                           from airbnb
                           where cleaning_fee != 'Not Specified'
                           group by cleaning_fee
                           order by count desc
                           limit 10;""")
        conn.commit()
        s = sand.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=['cleaning_fee', 'count'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['percentage'] = data['count'].apply(
            lambda x: str('{:.2f}'.format(x/55.55)) + '%')
        data['y'] = data['cleaning_fee'].apply(lambda x: str(x)+'`')
        return data

    def location():
        conn = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="sandeep",
                                port=5432,
                                database="PhonePe")
        sand = conn.cursor()
        sand.execute(f"""select host_id, country, longitude, latitude
                           from airbnb
                           group by host_id, country, longitude, latitude""")
        conn.commit()
        s = sand.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(
            s, columns=['Host ID', 'Country', 'Longitude', 'Latitude'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def feature_analysis():
        # PostgreSQL connection
        conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="sandeep",
            port=5432,
            database="PhonePe"
        )

        # Query the database
        query = """SELECT country, AVG(price) AS average_price
                    FROM airbnb
                    GROUP BY country;"""

        sand = conn.cursor()
        sand.execute(query)
        data = sand.fetchall()
        conn.commit()

        # Create a DataFrame from the fetched data
        df = pd.DataFrame(data, columns=['country', 'average_price'])

        # Read world countries shapefile
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

        # Merge the world data with the average_prices on 'name'
        world = world.merge(df, how='left', left_on='name', right_on='country')

        # Streamlit app
        st.title('Average Prices per Country Map')

        # Plotting the world map with hover functionality
        fig = px.choropleth(
            world,
            geojson=world.geometry,
            locations=world.index,
            color='country',
            hover_data={'average_price': True},
            projection='natural earth',
            title='Average Prices per Country'
        )

        #fig.update_geos(fitbounds="locations", visible=False)

        # Display the map in Streamlit
        st.plotly_chart(fig)      
        # vertical_bar chart
        property_type = feature.feature('property_type')
        plotly.vertical_bar_chart(df=property_type, x='property_type', y='count',
                                  text='percentage', color='#5D9A96', title='Property Type', title_x=0.43)

        # line & pie chart
        col1, col2 = st.columns(2)
        with col1:
            bed_type = feature.feature('bed_type')
            plotly.line_chart(df=bed_type, y='bed_type', x='count', text='percentage', color='#5cb85c',
                              textposition=[
                                  'top center', 'bottom center', 'middle right', 'middle right', 'middle right'],
                              title='Bed Type', title_x=0.50)
        with col2:  
            room_type = feature.feature('room_type')
            plotly.pie_chart(df=room_type, x='room_type',
                             y='count', title='Room Type', title_x=0.30)

        

        # line chart
        cancellation_policy = feature.feature('cancellation_policy')
        plotly.line_chart(df=cancellation_policy, y='cancellation_policy', x='count', text='percentage', color='#5D9A96',
                          textposition=['top center', 'top right',
                                        'top center', 'bottom center', 'middle right'],
                          title='Cancellation Policy', title_x=0.43)


        # line chart
        host_response_time = feature.feature('host_response_time')
        plotly.line_chart(df=host_response_time, y='host_response_time', x='count', text='percentage', color='#5cb85c',
                          textposition=['top center', 'top right',
                                        'top right', 'bottom left', 'bottom left'],
                          title='Host Response Time', title_x=0.43)

        # pie chart
        tab1, tab2, tab3 = st.tabs(
            ['Host is Superhost', 'Host has Profile Picture', 'Host Identity Verified'])
        with tab1:
            host_is_superhost = feature.feature('host_is_superhost')
            plotly.pie_chart(df=host_is_superhost, x='host_is_superhost',
                             y='count', title='Host is Superhost', title_x=0.39)
        with tab2:
            host_has_profile_pic = feature.feature('host_has_profile_pic')
            plotly.pie_chart(df=host_has_profile_pic, x='host_has_profile_pic',
                             y='count', title='Host has Profile Picture', title_x=0.37)
        with tab3:
            host_identity_verified = feature.feature('host_identity_verified')
            plotly.pie_chart(df=host_identity_verified, x='host_identity_verified',
                             y='count', title='Host Identity Verified', title_x=0.37)

        # vertical_bar,pie,map chart
        tab1, tab2, tab3 = st.tabs(['Market', 'Country', 'Location Exact'])
        with tab1:
            market = feature.feature('market', limit=12)
            plotly.vertical_bar_chart(df=market, x='market', y='count', text='percentage',
                                      color='#5D9A96', title='Market', title_x=0.43)
        with tab2:
            country = feature.feature('country')
            plotly.vertical_bar_chart(df=country, x='country', y='count', text='percentage',
                                      color='#5D9A96', title='Country', title_x=0.43)
        with tab3:
            is_location_exact = feature.feature('is_location_exact')
            plotly.pie_chart(df=is_location_exact, x='is_location_exact', y='count',
                             title='Location Exact', title_x=0.37)


# streamlit title, background color and tab configuration
streamlit_config()
st.write('')


with st.sidebar:


    option = option_menu(menu_title='', options=['Migrating to SQL', 'Features Analysis', 'Exit'],
                         icons=['database-fill', 'list-task', 'person-circle', 'sign-turn-right-fill'])
    col1, col2, col3 = st.columns([0.26, 0.48, 0.26])
    with col2:
        button = st.button(label='Submit')


if button and option == 'Migrating to SQL':
    st.write('')
    sql.create_table()
    sql.delete_table()
    sql.data_migration()
    st.success('Successfully Data Migrated to SQL Database')
    st.balloons()


elif option == 'Features Analysis':
    try:
        st.write('')
        feature.feature_analysis()

    except:
        col1, col2 = st.columns(2)
        with col1:
            st.info('SQL Database is Currently Empty')


elif option == 'Exit':
    st.write('')
    conn = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="sandeep",
                            port=5432,
                            database="PhonePe")
    sand = conn.cursor()
    conn.close()

    st.success('Thank you for your time. Exiting the application')
    st.balloons()

