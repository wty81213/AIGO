import plotly.figure_factory as ff
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression

def distplot(data,numeric_col,bin_size,group_col = None):
    df = data[~data[numeric_col].isnull()]
    histogram_data = []
    group_labels = []
    if group_col:
        group_labels = list(df[group_col].unique())
        for group in group_labels:
            histogram_data.append(df[numeric_col][df[group_col] == group].tolist())
    else:
        group_labels = [numeric_col]
        histogram_data.append(df[numeric_col])
    fig = ff.create_distplot(histogram_data, group_labels, bin_size = bin_size, show_rug = False)
    return fig

def scatter_plot(data,x,y,color = None):
    X = data[x].values.reshape(-1, 1)
    model = LinearRegression()
    model.fit(X, data[y])
    cor_values = np.round(data[[x,y]].corr().iloc[0,1],4)
    x_range = np.linspace(X.min(), X.max(), 100)
    y_range = model.predict(x_range.reshape(-1, 1))

    fig = px.scatter(data, x=x, y=y, color=color, opacity=0.65, title = '{} vs {}| cor = {}'.format(x,y,str(cor_values)))
    fig.add_traces(go.Scatter(x=x_range, y=y_range, name='Regression Fit'))
    fig.show()
