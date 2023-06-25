import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from folium import Icon, Map, Marker
from plotly.graph_objects import Figure
from polars import read_csv
from streamlit import sidebar as sb
from streamlit.delta_generator import DeltaGenerator
from streamlit_folium import st_folium as map


def dis(f: Figure, place: DeltaGenerator = st, rangeslider: bool = False):
    f = f.update_layout(
        margin=dict(l=0, r=0, b=0, t=0),
    )
    if rangeslider:
        f = f.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list(
                    [
                        dict(
                            count=1,
                            label='1m',
                            step='month',
                            stepmode='backward',
                        ),
                        dict(
                            count=3,
                            label='3m',
                            step='month',
                            stepmode='backward',
                        ),
                        dict(
                            count=6,
                            label='6m',
                            step='month',
                            stepmode='backward',
                        ),
                        dict(
                            count=1,
                            label='YTD',
                            step='year',
                            stepmode='todate',
                        ),
                        dict(
                            count=1,
                            label='1y',
                            step='year',
                            stepmode='backward',
                        ),
                        dict(step='all'),
                    ]
                )
            ),
        )
    place.plotly_chart(
        f,
        use_container_width=True,
    )


st.set_page_config(layout='wide')
st.markdown(
    """
<style>
div[data-testid="stExpander"] div[role="button"] p {
    font-size: 2rem;
}
div.stButton button {width: 100%;}
div.block-container {padding-top:2rem}
footer {visibility: hidden;}
@font-face {font-family: 'SF Pro Display';}
html, body, [class*="css"]  {font-family: 'SF Pro Display';}
thead tr th:first-child {display:none}
tbody th {display:none}
</style>
""",
    unsafe_allow_html=True,
)
cf = sb.expander('Settings')
nbins = cf.slider('Number of bins for histogram', 1, 1000, 50)
curve = cf.checkbox('Curve for line chart', value=True)

page = sb.selectbox(
    'Select a page',
    (
        'Exploratory Data Analysis',
        'RFM Analysis',
    ),
)
if page == 'Exploratory Data Analysis':
    view = sb.selectbox(
        'Select a view',
        (
            'Store',
            'Time',
            'Customer',
            'Product',
        ),
    )

    if view == 'Store':
        total_by_store = read_csv('b2c/total_by_store.csv')

        c1, c2 = st.columns(2)
        c1.subheader('Number of customer each store')
        dis(
            px.pie(
                total_by_store,
                values='customers',
                hover_name='address',
            ),
            c1,
        )
        c2.subheader('Sales each store')
        dis(
            px.pie(
                total_by_store,
                values='total',
                hover_name='address',
            ),
            c2,
        )
        # dis(
        #     px.scatter_geo(
        #         total_by_store,
        #         lat='latitude',
        #         lon='longitude',
        #         hover_name='address',
        #         size='total',
        #     ).update_geos(
        #         visible=True,
        #         resolution=50,
        #         scope='north america',
        #         subunitcolor='Blue',
        #         fitbounds='locations',
        #     )
        # )
        d = total_by_store.to_dicts()
        center = np.array(
            [
                [
                    d[i]['latitude'],
                    d[i]['longitude'],
                ]
                for i in range(len(d))
            ]
        ).mean(axis=0)
        m = Map(location=center, zoom_start=12)
        for i in range(len(d)):
            Marker(
                [
                    d[i]['latitude'],
                    d[i]['longitude'],
                ],
                tooltip=d[i]['address']
                + f' ({d[i]["customers"]} customers, {d[i]["total"]}$ in sales)',
                icon=Icon(
                    color='green',
                    icon='coffee',
                    prefix='fa',
                ),
            ).add_to(m)
        map(
            m,
            height=700,
            width=1200,
        )

    elif view == 'Time':
        st.header('Time')
        order_by_date = read_csv('b2c/order_by_date.csv')
        order_by_month = read_csv('b2c/order_by_month.csv')

        line_shape = 'spline' if curve else 'linear'
        t1, t2 = st.tabs(['By date', 'By month'])

        t1.subheader('Quantity sold by date')
        dis(
            px.line(
                order_by_date,
                x='date',
                y='quantity',
                height=700,
                line_shape=line_shape,
            ),
            t1,
            rangeslider=True,
        )
        t1.subheader('Sales by date')
        dis(
            px.line(
                order_by_date,
                x='date',
                y='total',
                height=700,
                line_shape=line_shape,
            ),
            t1,
            rangeslider=True,
        )
        t2.subheader('Quantity sold by month')
        dis(
            px.line(
                order_by_month,
                x='month',
                y='quantity',
                height=700,
                line_shape=line_shape,
            ),
            t2,
            rangeslider=True,
        )
        t2.subheader('Sales by month')
        dis(
            px.line(
                order_by_month,
                x='month',
                y='total',
                height=700,
                line_shape=line_shape,
            ),
            t2,
            rangeslider=True,
        )

    elif view == 'Customer':
        st.header('Customer')
        total_by_customer = read_csv('b2c/total_by_customer.csv')

        ex1 = st.expander('Sales distribution')
        ex1.subheader('Quantity sold')
        dis(
            px.histogram(
                total_by_customer,
                x='quantity',
            ),
            ex1,
        )
        ex1.subheader('Sales')
        dis(
            px.histogram(
                total_by_customer,
                x='total',
            ),
            ex1,
        )

        ex2 = st.expander('Age & gender distribution')
        ex2.subheader('Gender distribution')
        dis(
            px.pie(
                total_by_customer,
                names='gender',
            ),
            ex2,
        )
        ex2.subheader('Age distribution')
        dis(
            px.histogram(
                total_by_customer,
                x='age',
            ),
            ex2,
        )

        ex3 = st.expander('Relationships')
        ex3.subheader('Age & sales')
        dis(
            px.bar(
                total_by_customer,
                x='age',
                y='total',
                color='gender',
            ),
            ex3,
        )
        ex3.subheader('Age & gender')
        dis(
            px.violin(
                total_by_customer,
                x='gender',
                y='age',
                box=True,
            ),
            ex3,
        )

    elif view == 'Product':
        st.header('Product')
        total_by_product = read_csv('b2c/total_by_product.csv')

        ex1 = st.expander('Sales distribution')
        ex1.subheader('Quantity sold')
        dis(
            px.histogram(
                total_by_product,
                x='quantity',
                nbins=nbins,
            ),
            ex1,
        )
        ex1.subheader('Sales')
        dis(
            px.histogram(
                total_by_product,
                x='total',
                nbins=nbins,
            ),
            ex1,
        )

        ex2 = st.expander('Product distribution')
        ex2.subheader('Group')
        dis(
            px.pie(
                total_by_product,
                names='group',
            ),
            ex2,
        )
        ex2.subheader('Category')
        dis(
            px.pie(
                total_by_product,
                names='category',
            ),
            ex2,
        )
        ex2.subheader('Type')
        dis(
            px.pie(
                total_by_product,
                names='type',
            ),
            ex2,
        )
        ex2.subheader('Tax exempt')
        dis(
            px.pie(
                total_by_product,
                names='is_tax_exempt',
            ),
            ex2,
        )

        ex3 = st.expander('Relationships')
        ex3.subheader('Category & sales')
        dis(
            px.bar(
                total_by_product,
                x='group',
                y='total',
                color='category',
            ),
            ex3,
        )
        ex3.subheader('Unit cost & sales')
        dis(
            px.histogram(
                total_by_product,
                x='cost',
                y='total',
                color='group',
                nbins=nbins,
            ),
            ex3,
        )

elif page == 'RFM Analysis':
    st.title('Recency Frequency Monetary Analysis')
    view = sb.selectbox(
        'Select a view',
        (
            'Segment Analysis',
            'Segmentation Map',
        ),
    )
    if view == 'Segmentation Map':
        st.subheader('Customer Segmentation Map by RFM scores')

        segment_count = read_csv('b2c/segment_count.csv')

        dis(
            px.treemap(
                segment_count,
                path=['segment'],
                values='count',
                height=600,
            )
        )
        dis(
            px.pie(
                segment_count,
                values='count',
                names='segment',
            )
        )
    if view == 'Segment Analysis':
        st.header('Segment Analysis by RFM scores')

        rfm = pd.read_csv('b2c/segment.csv')
        st.subheader('All 3 metrics w.r.t each other')
        dis(
            px.scatter_3d(
                rfm,
                x='recency',
                y='frequency',
                z='monetary',
                color='segment',
                height=700,
            ).update_traces(
                marker_size=2,
            )
        )
        st.subheader('Recency w.r.t Frequency')
        dis(
            px.scatter(
                rfm,
                x='recency',
                y='frequency',
                color='segment',
                height=700,
            )
        )
        for i in [
            'recency',
            'frequency',
            'monetary',
        ]:
            st.subheader(f'Distribution of {i.capitalize()} on each segment')
            dis(
                px.box(
                    rfm,
                    x=i,
                    y='segment',
                    height=700,
                )
            )
