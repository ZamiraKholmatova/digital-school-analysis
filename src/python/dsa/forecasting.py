#%%
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt

#%%

# new_users = [
#     14,
#     7,
#     16,
#     523,
#     1108,
#     1476,
#     1236,
#     3136,
#     6227,
#     5396,
#     2981,
#     6153,
#     10868,
#     11309,
#     11357,
#     10699,
#     8716,
#     7086,
#     8094,
#     6255,
#     9541,
#     13162,
#     15316,
#     7500,
#     8506,
#     12423,
#     17334,
#     18123,
#     20385,
#     17125,
#     16874,
#     15042,
#     14715,
#     20443,
#     33841,
#     21999,
#     18766,
#     17305,
#     15499,
#     20089,
#     26089,
#     26823,
#     23314,
#     23810,
#     18155,
#     13124,
#     10432,
#     9807,
#     9413,
#     8979,
#     10519,
#     9176,
#     7709,
#     10332,
#     11728,
#     15170,
#     19087,
#     21496,
#     20776,
#     10484,
#     10358
# ]

# new_users = [
#     0.00,
#     114.85,
#     229.69,
#     344.54,
#     459.38,
#     574.23,
#     689.08,
#     803.92,
#     918.77,
#     1033.62,
#     1148.46,
#     1263.31,
#     1378.15,
#     1493,
#     3811.50,
#     6130.00,
#     8448.50,
#     10767.00,
#     13085.50,
#     15404.00,
#     17341.43,
#     19278.86,
#     21216.29,
#     23153.71,
#     25091.14,
#     27028.57,
#     28966.00,
#     33927.86,
#     38889.71,
#     43851.57,
#     48813.43,
#     53775.29,
#     58737.14,
#     63699.00,
#     71195.86,
#     78692.71,
#     86189.57,
#     93686.43,
#     101183.29,
#     108680.14,
#     116177.00,
#     125276.43,
#     134375.86,
#     143475.29,
#     152574.71,
#     161674.14,
#     170773.57,
#     179873.00,
#     188071.17,
#     196269.33,
#     204467.50,
#     212665.67,
#     220863.83,
#     229062,
#     234494.50,
#     239927.00,
#     245359.50,
#     250792,
#     255333.50,
# 259875.00,
# 264416.50,
# 268958
# ]

#%%
data = pd.read_csv("forecast.csv", parse_dates=["Дата"])
data = data.set_index("Дата")

new_users = data["Прирост в день"]
active_users = data["Интерполированные"]
#%%
class Oracle:
    def __init__(self, series, order=(10,2,10)) -> None:
        self.series = series
        self.model = ARIMA(series, order=order, freq="D")
        self.model_fit = self.model.fit()
        pass

    def print_summary(self):
        self.model_fit.summary()

    def residuals(self):
        residuals = pd.DataFrame(self.model_fit.resid)
        residuals.plot()
        plt.show()
        residuals.plot(kind='kde')
        plt.show()
        print(residuals.describe())

    def forecast(self, date):
        new_dates = pd.date_range(start = self.series.index[-1],
           end=date, freq ='D').tolist()[1:]
        need_samples = len(new_dates)
        forecast = self.model_fit.get_forecast(need_samples)
        conf_int = forecast.conf_int(alpha=0.60)
        print()
        self.predicted = pd.Series(forecast.predicted_mean, index=new_dates)
        self.lower_bound = pd.Series(conf_int.values[:, 0], index=new_dates)
        self.upped_bound = pd.Series(conf_int.values[:, 1], index=new_dates)

    def plot(self):
        plt.figure(figsize=(12,5), dpi=100)
        plt.plot(self.series, label='training')
        plt.plot(self.predicted, label='forecast')
        plt.fill_between(self.lower_bound.index, self.lower_bound, self.upped_bound, 
                        color='k', alpha=.15)
        plt.title('Forecast vs Actuals')
        plt.legend(loc='upper left', fontsize=8)
        plt.show()

        print(self.predicted[-1],self.predicted[-1]- self.lower_bound[-1],self.predicted[-1]- self.upped_bound[-1])

    def plot_cummulative(self):
        agg_conf_l = []
        agg_conf_u = []
        agg_conf_p = []

        full_dates = self.series.index.append(self.predicted.index)

        for i in range(len(full_dates)):
            if i < len(self.series):
                if i == 0:
                    value = new_users[i]
                else:
                    value = agg_conf_p[-1] + new_users[i]
                agg_conf_l.append(value)
                agg_conf_u.append(value)
                agg_conf_p.append(value)
            else:
                agg_conf_l.append(agg_conf_l[-1] + self.lower_bound[full_dates[i]])
                agg_conf_u.append(agg_conf_u[-1] + self.upped_bound[full_dates[i]])
                agg_conf_p.append(agg_conf_p[-1] + self.predicted[full_dates[i]])

        agg_conf_l = pd.Series(agg_conf_l, index=full_dates)
        agg_conf_u = pd.Series(agg_conf_u, index=full_dates)
        agg_conf_p = pd.Series(agg_conf_p, index=full_dates)

        plt.figure(figsize=(12,5), dpi=100)
        plt.plot(agg_conf_p, label='training')
        # plt.plot(yhat, label='forecast')
        plt.fill_between(agg_conf_l.index, agg_conf_l, agg_conf_u, 
                        color='k', alpha=.15)
        plt.title('Forecast vs Actuals')
        plt.legend(loc='upper left', fontsize=8)
        plt.show()

        print(agg_conf_p[-1],agg_conf_p[-1]- agg_conf_l[-1],agg_conf_p[-1]- agg_conf_u[-1])


new_users_predictor = Oracle(new_users, order=(10,1,10))
# new_users_predictor.residuals()
new_users_predictor.forecast("2021-12-27")
new_users_predictor.plot_cummulative()


active_users_predictor = Oracle(active_users, order=(10,1,10))
# new_users_predictor.residuals()
active_users_predictor.forecast("2021-12-27")
active_users_predictor.plot()

#%%




# Plot
plt.figure(figsize=(12,5), dpi=100)
plt.plot(new_users, label='training')
plt.plot(yhat, label='forecast')
plt.fill_between(lower_series.index, lower_series, upper_series, 
                 color='k', alpha=.15)
plt.title('Forecast vs Actuals')
plt.legend(loc='upper left', fontsize=8)
plt.show()

agg_conf_l = []
agg_conf_u = []
agg_conf_p = []
index = []
# agg_conf_l = []

for i in range(len(new_users)+len(yhat)-1):
    if i < len(new_users):
        if i == 0:
            value = new_users[i]
        else:
            value = agg_conf_p[-1] + new_users[i]
        agg_conf_l.append(value)
        agg_conf_u.append(value)
        agg_conf_p.append(value)
    else:
        agg_conf_l.append(agg_conf_l[-1] + lower_series.loc[i])
        agg_conf_u.append(agg_conf_u[-1] + upper_series.loc[i])
        agg_conf_p.append(agg_conf_p[-1] + yhat.loc[i])
    index.append(i)

agg_conf_l = pd.Series(agg_conf_l, index=index)
agg_conf_u = pd.Series(agg_conf_u, index=index)
agg_conf_p = pd.Series(agg_conf_p, index=index)

plt.figure(figsize=(12,5), dpi=100)
plt.plot(agg_conf_p, label='training')
# plt.plot(yhat, label='forecast')
plt.fill_between(agg_conf_l.index, agg_conf_l, agg_conf_u, 
                 color='k', alpha=.15)
plt.title('Forecast vs Actuals')
plt.legend(loc='upper left', fontsize=8)
plt.show()
# %%
yhat.iloc[-1], yhat.iloc[-1] - lower_series.iloc[-1], yhat.iloc[-1] - upper_series.iloc[-1]
# %%

# %%
agg_conf_p.iloc[-1], agg_conf_p.iloc[-1]-  agg_conf_l.iloc[-1],agg_conf_p.iloc[-1]- agg_conf_u.iloc[-1]
# %%
