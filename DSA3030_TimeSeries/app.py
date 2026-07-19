import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
from statsmodels.tsa.seasonal import seasonal_decompose
from prophet import Prophet
import datetime
from PIL import Image

# ---------------------------------------------------------
# 1. PREMIUM PAGE CONFIGURATION
# ---------------------------------------------------------
st.set_page_config(
    page_title="Forex Analytics: USD/KES",
    page_icon="💱",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Premium Custom CSS
st.markdown("""
<style>
    /* Main Background & Text */
    .stApp {
        background-color: #0E1117;
        color: #FAFAFA;
        font-family: 'Inter', sans-serif;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #00FFC6 !important;
        font-family: 'Outfit', sans-serif;
        font-weight: 600;
        letter-spacing: -0.5px;
    }
    
    /* Glassmorphism Metrics */
    div[data-testid="stMetricValue"] {
        color: #00FFC6;
        font-size: 2.2rem !important;
        font-weight: 700;
    }
    div[data-testid="stMetricLabel"] {
        color: #A0AEC0;
        font-size: 1rem !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #1A202C !important;
        border-right: 1px solid #2D3748;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(0, 255, 198, 0.1);
        border-bottom: 3px solid #00FFC6 !important;
        color: #00FFC6 !important;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# 2. DATA FETCHING FUNCTION (FROM SPARK/HADOOP OUTPUT)
# ---------------------------------------------------------
import os
import glob

@st.cache_data(ttl=3600)
def load_data():
    """Reads the processed Forex data saved by Apache Spark."""
    data_dir = "data/processed_forex.csv"
    
    if not os.path.exists(data_dir):
        st.error("⚠️ Spark output not found! Please run the Kafka Producer and Spark Processor first.")
        return pd.DataFrame()
        
    # Read all CSV parts output by Spark
    all_files = glob.glob(os.path.join(data_dir, "*.csv"))
    if not all_files:
        st.error("⚠️ No CSV data found in the Spark output directory.")
        return pd.DataFrame()
        
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    
    # Ensure datatypes and sort chronologically
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date').reset_index(drop=True)
    
    # Clean timezone info and rename to match previous logic
    df.rename(columns={'date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
    df['Date'] = df['Date'].dt.tz_localize(None)
    
    return df

# ---------------------------------------------------------
# 3. SIDEBAR & APP HEADER
# ---------------------------------------------------------
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/4/49/Flag_of_Kenya.svg/2560px-Flag_of_Kenya.svg.png", width=100)
st.sidebar.title("💱 KES Forex Analytics")
st.sidebar.markdown("---")
st.sidebar.markdown("**Course:** DSA3030 Time Series")
st.sidebar.markdown("**Assignment:** Semester Term Paper")
st.sidebar.markdown("**Pipeline:** Kafka -> Spark -> Hadoop -> Streamlit")
st.sidebar.markdown("---")

# Fetch Data
with st.spinner("Loading Processed Data from Spark..."):
    data = load_data()
    
if data.empty:
    st.stop()

st.title("💱 Dynamic Macroeconomic Forecasting: USD to KES")
st.markdown("*An interactive application analyzing the historical trends, seasonality, and predictive future of the Kenyan Shilling.*")

# ---------------------------------------------------------
# 4. TABS SETUP
# ---------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "📄 Academic Report", 
    "📈 Exploratory Data Analysis", 
    "🧩 Time Series Decomposition", 
    "🔮 ML Forecasting (Prophet)"
])

# =========================================================
# TAB 1: ACADEMIC REPORT
# =========================================================
with tab1:
    st.markdown("## Application of Time Series and Forecasting in Macroeconomics")
    st.markdown("### 1. Introduction")
    st.write("""
    Time series forecasting is a crucial statistical technique used to predict future values based on previously observed, chronologically ordered data. In the realm of macroeconomics and global finance, forecasting foreign exchange (Forex) rates is one of the most challenging and essential applications of this methodology. 
    
    This term paper investigates the exchange rate between the United States Dollar (USD) and the Kenyan Shilling (KES). The KES has experienced significant volatility due to global macroeconomic shocks, domestic monetary policy, and changing import/export dynamics. Accurate forecasting of this currency pair allows importers, exporters, the Central Bank of Kenya, and foreign investors to hedge against currency risk and make informed fiscal decisions.
    """)
    
    st.markdown("### 2. The Dataset")
    st.write("""
    Unlike static datasets, this research employs a dynamic data pipeline connecting directly to the global financial markets via the `yfinance` API. The target variable is the daily closing exchange rate of `USD/KES` (Ticker: `KES=X`). 
    
    The dataset captures Open, High, Low, and Close (OHLC) prices, as well as trading volume. By utilizing up-to-the-minute live data, our models are inherently protected against data staleness—a critical flaw in traditional academic papers relying on CSV exports.
    """)

    st.markdown("### 3. Exploratory Data Analysis (EDA)")
    st.write("""
    The Exploratory Data Analysis (EDA) phase is designed to identify the macro-level behavior of the currency. The interactive visualisations in the EDA tab expose long-term depreciation cycles of the Kenyan Shilling.
    
    We employ Simple Moving Averages (SMA)—specifically the 50-day and 200-day SMAs—to smooth out daily noise and identify critical 'crossovers' indicating structural shifts in market momentum. Additionally, the distribution of daily returns highlights the volatility and non-stationary nature of the exchange rate.
    """)

    st.markdown("### 4. Time Series Decomposition")
    st.write("""
    To deeply understand the structural components driving the USD/KES exchange rate, we utilize Seasonal Decomposition. Financial time series are typically composed of three distinct mathematical elements:
    
    1. **Trend:** The underlying long-term trajectory of the currency (e.g., the steady depreciation of the KES over the last decade).
    2. **Seasonality:** Periodic fluctuations that repeat at regular intervals. In Forex, this can be driven by corporate tax deadlines, seasonal agricultural exports (like tea and coffee in Kenya), or holiday remittance inflows.
    3. **Residuals (Noise):** The random, unpredictable market shocks caused by breaking news, political events, or sudden geopolitical crises.
    
    By separating the data into these three distinct components, we strip away the noise to uncover the true cyclical nature of the Kenyan economy.
    """)

    st.markdown("### 5. Machine Learning Forecasting with Prophet")
    st.write("""
    Traditional statistical models like ARIMA often struggle with the extreme non-linear volatility, missing days (weekends/holidays), and structural breaks inherent in global financial markets. To resolve this, this project utilizes **Prophet**, an advanced additive regression model developed by Meta's Core Data Science team.
    
    Prophet is exceptionally robust against missing data and shifts in the trend. It models the time series as:
    
    `y(t) = g(t) + s(t) + h(t) + e(t)`
    
    Where:
    - **g(t)** is the trend function representing non-periodic changes.
    - **s(t)** represents periodic changes (seasonality).
    - **h(t)** represents the effects of holidays.
    - **e(t)** is the error term.
    
    The interactive dashboard visualizes the model's predictions, bounded by an 80% confidence interval, allowing stakeholders to estimate the most probable future trajectory of the USD/KES exchange rate over the upcoming quarter.
    """)
    
    st.markdown("### 6. Conclusion")
    st.write("""
    By blending modern Software Engineering with advanced Data Science, this application provides a live, interactive environment for macroeconomic analysis. Time series forecasting proves to be an indispensable tool for deciphering historical market behavior and providing actionable, data-driven foresight into the future stability of the Kenyan Shilling.
    """)


# =========================================================
# TAB 2: EXPLORATORY DATA ANALYSIS (EDA)
# =========================================================
with tab2:
    st.header("📈 Exploratory Data Analysis (EDA)")
    
    col1, col2, col3 = st.columns(3)
    latest_close = data['Close'].iloc[-1]
    previous_close = data['Close'].iloc[-2]
    pct_change = ((latest_close - previous_close) / previous_close) * 100
    
    col1.metric("Current USD/KES", f"Ksh {latest_close:.2f}", f"{pct_change:.2f}%")
    col2.metric("52-Week High", f"Ksh {data['High'].tail(365).max():.2f}")
    col3.metric("52-Week Low", f"Ksh {data['Low'].tail(365).min():.2f}")
    
    st.markdown("### Historical Price Action & Moving Averages")
    
    # Calculate Moving Averages
    df_eda = data.copy()
    df_eda['50_SMA'] = df_eda['Close'].rolling(window=50).mean()
    df_eda['200_SMA'] = df_eda['Close'].rolling(window=200).mean()
    
    fig_eda = go.Figure()
    fig_eda.add_trace(go.Scatter(x=df_eda['Date'], y=df_eda['Close'], mode='lines', name='Daily Close', line=dict(color='#00FFC6', width=2)))
    fig_eda.add_trace(go.Scatter(x=df_eda['Date'], y=df_eda['50_SMA'], mode='lines', name='50-Day SMA', line=dict(color='#FF2A6D', width=1, dash='dash')))
    fig_eda.add_trace(go.Scatter(x=df_eda['Date'], y=df_eda['200_SMA'], mode='lines', name='200-Day SMA', line=dict(color='#05D5FF', width=1, dash='dot')))
    
    fig_eda.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis_title="Date",
        yaxis_title="Exchange Rate (KES)",
        hovermode="x unified",
        height=500
    )
    st.plotly_chart(fig_eda, use_container_width=True)
    
    st.markdown("### Daily Returns Distribution")
    df_eda['Daily_Return'] = df_eda['Close'].pct_change() * 100
    fig_hist = px.histogram(df_eda, x="Daily_Return", nbins=100, color_discrete_sequence=['#00FFC6'])
    fig_hist.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis_title="Daily Return (%)",
        yaxis_title="Frequency",
        height=400
    )
    st.plotly_chart(fig_hist, use_container_width=True)

# =========================================================
# TAB 3: DECOMPOSITION
# =========================================================
with tab3:
    st.header("🧩 Time Series Decomposition")
    st.write("We use an **additive decomposition** model to extract the underlying Trend, Seasonality, and Random Noise from the exchange rate data.")
    
    # Need to handle missing days for statsmodels (interpolate weekends)
    df_decomp = data[['Date', 'Close']].copy()
    df_decomp.set_index('Date', inplace=True)
    df_decomp = df_decomp.asfreq('D')
    df_decomp['Close'] = df_decomp['Close'].interpolate(method='time')
    
    try:
        # Period = 365 for daily data with annual seasonality
        decomposition = seasonal_decompose(df_decomp['Close'], model='additive', period=365)
        
        fig_trend = px.line(decomposition.trend, color_discrete_sequence=['#FF2A6D'], title="1. Underlying Trend (Macro Economic Trajectory)")
        fig_seasonal = px.line(decomposition.seasonal, color_discrete_sequence=['#05D5FF'], title="2. Annual Seasonality (Cyclical Events)")
        fig_resid = px.scatter(decomposition.resid, color_discrete_sequence=['#A0AEC0'], title="3. Residuals (Random Market Shocks)")
        
        for fig in [fig_trend, fig_seasonal, fig_resid]:
            fig.update_layout(
                template="plotly_dark", plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False, height=300, xaxis_title="", yaxis_title="Value"
            )
            st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        st.error(f"Not enough data points to decompose with a 365-day period. Please select a longer historical range in the sidebar. Error: {e}")

# =========================================================
# TAB 4: FORECASTING
# =========================================================
with tab4:
    st.header("🔮 Machine Learning Forecasting (Prophet)")
    st.write("Using Meta's **Prophet** algorithm, we forecast the future trajectory of the USD/KES exchange rate.")
    
    forecast_days = st.slider("Select Forecast Horizon (Days):", min_value=30, max_value=365, value=90, step=30)
    
    # Prepare data for Prophet
    df_prophet = data[['Date', 'Close']].rename(columns={'Date': 'ds', 'Close': 'y'})
    
    with st.spinner(f"Training Prophet model on {len(df_prophet)} historical data points..."):
        m = Prophet(daily_seasonality=False, yearly_seasonality=True, weekly_seasonality=True, changepoint_prior_scale=0.05)
        m.fit(df_prophet)
        
        future = m.make_future_dataframe(periods=forecast_days)
        forecast = m.predict(future)
    
    st.success("Model Training Complete!")
    
    # Interactive Plotly chart for Prophet
    fig_forecast = go.Figure()
    
    # Historical Data
    fig_forecast.add_trace(go.Scatter(x=df_prophet['ds'], y=df_prophet['y'], mode='markers', name='Actual Data', marker=dict(color='white', size=2)))
    
    # Forecast Line
    fig_forecast.add_trace(go.Scatter(x=forecast['ds'], y=forecast['yhat'], mode='lines', name='Forecasted Trend', line=dict(color='#00FFC6', width=2)))
    
    # Confidence Interval
    fig_forecast.add_trace(go.Scatter(
        name='Upper Bound', x=forecast['ds'], y=forecast['yhat_upper'], mode='lines',
        marker=dict(color="#444"), line=dict(width=0), showlegend=False
    ))
    fig_forecast.add_trace(go.Scatter(
        name='Lower Bound', x=forecast['ds'], y=forecast['yhat_lower'], mode='lines',
        marker=dict(color="#444"), line=dict(width=0), fillcolor='rgba(0, 255, 198, 0.2)', fill='tonexty', showlegend=False
    ))
    
    fig_forecast.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis_title="Date",
        yaxis_title="Exchange Rate (KES)",
        hovermode="x unified",
        height=600,
        title=f"USD/KES Forecast for the next {forecast_days} Days"
    )
    
    st.plotly_chart(fig_forecast, use_container_width=True)
    
    st.markdown("### Forecast Data Table")
    st.dataframe(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(forecast_days).rename(columns={
        'ds': 'Date', 'yhat': 'Predicted Value', 'yhat_lower': 'Lower Confidence Bound', 'yhat_upper': 'Upper Confidence Bound'
    }).set_index('Date'))
