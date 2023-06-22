import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from folium import Map, Marker
from plotly.graph_objects import Figure
from polars import read_csv
from streamlit import sidebar as sb
from streamlit_folium import st_folium as map


def dis(f: Figure, sidebar=False):
    if sidebar:
        sb.plotly_chart(
            f.update_layout(
                margin=dict(l=0, r=0, b=0, t=0),
            ),
            use_container_width=True,
        )
    else:
        st.plotly_chart(
            f.update_layout(
                margin=dict(l=0, r=0, b=0, t=0),
            ),
            use_container_width=True,
        )


def rangeslider(f: Figure):
    st.plotly_chart(
        f.update_xaxes(
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
        ),
        use_container_width=True,
    )


st.set_page_config(layout='wide')
st.markdown(
    """
<style>
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

page = sb.selectbox(
    'Select a page',
    (
        'Exploratory Data Analysis',
        'RFM Analysis',
    ),
)
if page == 'Exploratory Data Analysis':
    st.title('Exploratory Data Analysis')

    view = sb.selectbox(
        'Select a view',
        (
            'Time view',
            'Store view',
        ),
    )

    if view == 'Store view':
        st.subheader('Number of customer per store')

        customer_per_store = read_csv('b2c/customer_per_store.csv')
        dis(
            px.pie(
                customer_per_store,
                names='store',
                values='count',
                hover_name='address',
            )
        )
        # dis(
        #     px.scatter_geo(
        #         customer_per_store,
        #         lat='latitude',
        #         lon='longitude',
        #         hover_name='address',
        #         size='count',
        #     ).update_geos(
        #         visible=True,
        #         resolution=50,
        #         scope='north america',
        #         subunitcolor='Blue',
        #         fitbounds='locations',
        #     )
        # )
        d = customer_per_store.to_dicts()
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
                tooltip=d[i]['address'] + f' ({d[i]["count"]} customers)',
            ).add_to(m)
        map(
            m,
            height=700,
            width=1200,
        )

    if view == 'Time view':
        st.header('Time view')

        order_by_date = read_csv('b2c/order_by_date.csv')

        st.subheader('Quantity sold by date')
        rangeslider(
            px.line(
                order_by_date,
                x='date',
                y='quantity',
                height=700,
                line_shape='spline',
            )
        )
        st.subheader('Sales by date')
        rangeslider(
            px.line(
                order_by_date,
                x='date',
                y='total',
                height=700,
                line_shape='spline',
            )
        )

if page == 'RFM Analysis':
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
