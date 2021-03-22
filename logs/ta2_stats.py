#!/usr/bin/env python
# coding: utf-8

#pandas use to convert data in tabular form
import pandas as pd
#lib for graph
import matplotlib.pyplot as plt
import time
import datetime
from datetime import datetime
from decimal import Decimal
import math

#importing data
df = pd.read_csv('backtesting_trades.csv',sep='\s*,\s*', engine = 'python')
df1 = pd.read_csv('latest_bt_df.csv',sep='\s*,\s*', engine = 'python')

#stddev of 3min closes of df1 and price change
candlestdev = df1['close'].std()
candlemean = df1['close'].mean()
candlevol = candlestdev/candlemean
candlevol = round(candlevol,3)
start_close_df1 = df1['close'].iloc[0]
end_close_df1 = df1['close'].iloc[-1]
price_chg = 100*(end_close_df1 - start_close_df1)/start_close_df1
price_chg = round(price_chg, 2)


#tradesize	pos	fees	pnl	usdbal
#delete rows of no trades, ie sys startup and reindex
df = df.dropna(subset=['time_candle'])
df.index =range(len(df))

#get only last backtest to df since orig file is appended
last_index = df.apply(pd.Series.last_valid_index)['symbol']
df = df.loc[last_index:,:]
df.index =range(len(df))
#final.to_csv('lastresult.csv',index=False)

symbolname = df.loc[0,'symbol']
openlongs = 0
openshorts = 0
asset = 'futures'
# asset = input('asset type?')
if asset == 'spot':
    fee = .00075
elif asset == 'futures':
    fee = .0004


#analyze from user-defined startdate
# startunix = input('start date (dd/mm/yyyy) or none')
# startunix = time.mktime(datetime.datetime.strptime(startunix, "%d/%m/%Y").timetuple())
# print ('start unix', startunix)

#get times of 1st and last trade
starttime = df['time_candle'].iloc[1]
endtime = df['time_candle'].iloc[-1]
backtestran = df['time_candle'].iloc[0]
startdate = datetime.utcfromtimestamp(starttime).strftime('%Y-%m-%d %H:%M:%S')
enddate = datetime.utcfromtimestamp(endtime).strftime('%Y-%m-%d %H:%M:%S')
backtestrandate = datetime.utcfromtimestamp(backtestran).strftime('%Y-%m-%d %H:%M:%S')
# ndays = (endtime - starttime)/86400

#set starting bal and fees
starting_bal = 10000
#fee = .00075

#created all new columns with initial value 0
df['usdbal'] = starting_bal
df['tradesize']=0
df['tokenbal'] = 0
df['fees'] = 0
df['pnl'] = 0
df['hold'] = 0
df['roi per trade'] = 0
df['change in willr'] = 0

#temporary list to store data of calculation
usdbal_list = [starting_bal]
tradesize_list = [0]
tokenbal_list = [0]
fees_list = [0]
pnl_list = [0]
hold_list = [0]
#hold list for wins and losses
win_hold_list= []
loss_hold_list =[]
roipertrade_list = [0]
wins = 0
losses = 0
win_list = []
loss_list = []
change_willr_list = [0]

#looping through the data
for i in range(1, len(df)):
    timeclose = df.loc[i,'time_candle']
    position = df.loc[i,'position']
    action = df.loc[i,'action']
    price = df.loc[i,'price']

    willr = df.loc[i, 'willr']
    willr_prev = df.loc[i-1, 'willr']
    change_will_i = 0

    timeopen = df.loc[i-1,'time_candle']
    prev_action = df.loc[i-1,'action']
    prev_price = df.loc[i-1,'price']
    prev_usdbal = usdbal_list[i-1]
    prev_tradesize = tradesize_list[i-1]
    prev_tokenbal = tokenbal_list[i-1]
    hold = None
    starttradebal = usdbal_list[i-2]


#calc tradesize
    if action == 'Open':
        if position == 'Long':
            tradesize_i = prev_usdbal/price
            openlongs = openlongs + 1
        else:
            tradesize_i = - prev_usdbal/price
            openshorts = openshorts + 1
    else:
        tradesize_i = -prev_tradesize
        hold = timeclose  - timeopen


    tradesize_list.append(tradesize_i)
    hold_list.append(hold)

#calc pos
    tokenbal_i = prev_tokenbal + tradesize_i
    tokenbal_list.append(tokenbal_i)

#calc fee
    nominal_value = abs(tradesize_i*price)
    fees_i = fee*nominal_value
    fees_list.append(fees_i)

#calc pnl and willr change
    if action == 'Close':
        if position == 'Long':
            change_will_i = willr - willr_prev
        else:
            change_will_i = willr_prev - willr
    
            
        pnl_i = -tradesize_i*(price - prev_price)
        roipertrade_i = pnl_i/starttradebal
        roipertrade_i = round(roipertrade_i,4)
        if roipertrade_i >0:
            wins = wins + 1
            win_list.append(roipertrade_i)
        else:
            losses = losses + 1
            loss_list.append(roipertrade_i)
    else:
        pnl_i = 0
        roipertrade_i = None
    pnl_list.append(pnl_i)
    roipertrade_list.append(roipertrade_i)

    change_willr_list.append(change_will_i)

#calc usdbal
    usdbal_i = prev_usdbal - fees_i + pnl_i
    usdbal_list.append(usdbal_i)

#Assign list to actual columns
df['usdbal'] = usdbal_list
df['tradesize'] = tradesize_list
df['tokenbal'] = tokenbal_list
df['fees'] = fees_list
df['pnl'] = pnl_list
df['hold'] = hold_list
df['roi per trade'] = roipertrade_list
df['change in willr'] = change_willr_list



#check corr between hold and roiper
df_new = df[['hold', 'roi per trade','change in willr']].copy()
#calc avg hold stats for wins and losses
for i in range(1, len(df_new)):
    hold = df_new.loc[i,'hold']
    roi = df_new.loc[i,'roi per trade']
    if roi > 0:
        win_hold_list.append(hold)
    else:
        loss_hold_list.append(hold)    


df_new = df_new.dropna(subset=['hold'])
df_new.index =range(len(df_new))
corr_holdroi = round(df_new['hold'].corr(df_new['roi per trade']),2)
corr_holdroi = str(corr_holdroi)

ending_bal = df['usdbal'].iloc[-1]
totalopens = openlongs +openshorts
avghold = df['hold'].mean()
avghold_mins = avghold/60
avgroipertrade = df['roi per trade'].mean()

#create csv of new table
df.to_csv('output_backtest_results.csv')
df_new.to_csv('hold_vs_roi.csv')


#output stats
print('')
print ('-----BACKTEST RESULTS-----')
print(symbolname)
print('From', startdate)
print('To', enddate)
print('starting bal', '{0:.2f}'.format(starting_bal))
print('ending bal', '{0:.2f}'.format(ending_bal))
print('stdev, price chg%')
print(candlevol, price_chg, '%')
roi = (ending_bal/starting_bal - 1)
roi_perc = roi*100
roi = str(round(roi,3))



#print ('roi', roi,'x')
#print ('price chg', '{0:.4f}'.format(price_chg))
#print ('avg roi per trade', '{0:.4f}'.format(avgroipertrade))
#print ('openlongs/openshort', openlongs, '/', openshorts)
#print ('open long/short %', '{0:.2f}'.format(openlongs/totalopens), '/', '{0:.2f}'.format(openshorts/totalopens))
# print('hold_list', hold_list)
#print('avghold', '{0:.2f}'.format(avghold), 'seconds')
#print('avghold', '{0:.2f}'.format(avghold_mins),'minutes')
#print ('correlation:hold vs roi', corr_holdroi)

#plt.style.use('ggplot')
#df['usdbal'].plot(figsize = (18, 6))
#plt.title(symbolname + ' From ' + startdate + ' To ' + enddate + '    ROI:  ' + roi + ' open long/short%' + '{0:.2f}'.format(openlongs/totalopens) + '/' + '{0:.2f}'.format(openshorts/totalopens) + ' hold vs roi corr ' + corr_holdroi)

# plt.show()
#plt.savefig(symbolname + backtestrandate + '.png')

#get daily balances/rois/sharpe
df_daily = df[['time_candle', 'usdbal']].copy()
day1 = df_daily['time_candle'][1]
day1bal = df_daily['usdbal'][1]
days_list = [day1]
EODbal_list = [day1bal]
dailychg_list =[0]

# print (day1)
nextday = day1 + 86400
# print (nextday)
for i in range(1, len(df_daily)):
    timecandle_i = df_daily.loc[i,'time_candle']
    usdbal_i = df_daily.loc[i,'usdbal']
    #if row past the next day, get prev row's data
    if timecandle_i > nextday:
        day = df_daily.loc[i-1,'time_candle']
        EOD_bal = df_daily.loc[i-1,'usdbal']
        nextday = nextday + 86400
        days_list.append(day)
        EODbal_list.append(EOD_bal)

data = {'EOD time_candle': days_list,
        'EOD bal': EODbal_list}
daily_returns = pd.DataFrame(data)

#calc daily pnl
for i in range(1, len(daily_returns)):
    bal_i = daily_returns.loc[i, 'EOD bal']
    prev_bal = daily_returns.loc[i-1, 'EOD bal']
    pnl = (bal_i - prev_bal)/prev_bal
    dailychg_list.append(pnl)
daily_returns['daily chg'] = dailychg_list

daily_returns.to_csv('daily_returns.csv')

#sharpe ann_ratio
#min, max, avg daily return
min_return = min(dailychg_list)
max_return = max(dailychg_list)
mean_return = sum(dailychg_list)/len(dailychg_list)
#print ('min/max/avg of daily returns', '{0:.4f}'.format(min_return),'/', '{0:.4f}'.format(max_return),'/', '{0:.4f}'.format(mean_return))
#sharpe_ann = (expreturn_ann - riskfreerate)/std _ann
rf = 0
mean_return_ann =  365*mean_return
std_daily = daily_returns['daily chg'].std()
std_ann = std_daily*math.sqrt(365)
sharpe_ann = (mean_return_ann - rf)/std_ann
total_scalps = wins + losses
#std_ann = dailystd*sqrt(365)

minwin = round(min(win_list),4)
maxwin = round(max(win_list),4)
avgwin = round(sum(win_list)/len(win_list),4)
MAXloss = round(min(loss_list),4)
MINloss = round(max(loss_list),4)
avgloss = round(sum(loss_list)/len(loss_list),4)

print('')

print ('-----SUMMARY------')
print ('roi %/avgroipertrade/annualized Sharpe/wins/losses%/n scalps/wins(min/max/avg)/losses(MAX/min/avg)...price chg/vola')
print('{0:.1f}'.format(roi_perc), '% / ',
      '{0:.4f}'.format(avgroipertrade), ' / ',
      '{0:.2f}'.format(sharpe_ann),' / (',
      '{0:.2f}'.format(wins/total_scalps),'/',
      '{0:.2f}'.format(losses/total_scalps),')/ ',
       total_scalps, '/ (',
       maxwin, '/',
       minwin, '/',
       avgwin,')/(',
       MAXloss, '/',
       MINloss, '/',
       avgloss,')...',
       price_chg, '%/',
       candlevol
       )

#print ('win hold list')
#print (win_hold_list)
#print ('loss hold list')
#print (loss_hold_list)



#print ('annualized Sharpe', '{0:.4f}'.format(sharpe_ann))

#print ('wins%/losses%', '{0:.2f}'.format(wins/total_scalps),'/', '{0:.2f}'.format(losses/total_scalps))
#print ('total scalps', total_scalps)

