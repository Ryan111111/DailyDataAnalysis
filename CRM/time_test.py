import datetime

order_day_time = '6:00:00'
order_day_std_time = '00:00:00'
order_day_seconds = (datetime.datetime.strptime(order_day_time,'%H:%M:%S')-datetime.datetime.strptime(order_day_std_time,'%H:%M:%S')).seconds
print(order_day_seconds)