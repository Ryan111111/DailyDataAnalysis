import pandas as pd
import datetime


now_date = datetime.datetime.now().strftime('%Y-%m-%d')
now_date = datetime.datetime.strptime(now_date, "%Y-%m-%d")

meituan_order_data = pd.read_excel('meituan_order_yiwu20170123.xlsx')
meituan_order_data = pd.DataFrame(meituan_order_data)
meituan_order_data.columns = ['order_id','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','order_time','ab','ac','ad','ae','af','ag','ah','ai','phone','ak','al','am','an','ao','ap','aq']



def get_time_differ(meituan_order_data):
    '''
    获取订单时间的信息，并加入到整体数据中
    :return:
    '''
    days_differ_count = []
    order_weekdays_all = []
    order_day_time_all = []

    for i in range(len(meituan_order_data)):
        order_time = meituan_order_data.iloc[i,26]
        order_date = str(order_time).split(' ')[0]

        order_date = datetime.datetime.strptime(order_date, "%Y-%m-%d")  # 订单日期与当前时间之差
        days_differ = int((order_date - now_date).days)
        days_differ_count.append(days_differ)

        order_weekday = order_date.weekday() + 1                         # 订单属于周几
        order_weekdays_all.append(order_weekday)

        order_day_time = str(order_time).split(' ')[1]                   # 订单时间段
        order_day_std_time = '00:00:00'
        order_day_seconds = (datetime.datetime.strptime(order_day_time, '%H:%M:%S') - datetime.datetime.strptime(order_day_std_time,'%H:%M:%S')).seconds
        if order_day_seconds > 36000 and order_day_seconds < 46800:
            order_day_time_all.append(1)  # 午餐
        elif order_day_seconds > 59400 and order_day_seconds < 72000:
            order_day_time_all.append(2)  # 晚餐
        elif (order_day_seconds > 75600 and order_day_seconds < 86399) or order_day_seconds < 21600:
            order_day_time_all.append(3)  # 宵夜
        else:
            order_day_time_all.append(4)  # 非指定时间段

    meituan_order_data.insert(2, "days_differ_count", days_differ_count)
    meituan_order_data.insert(3, "order_weekday", order_weekdays_all)
    meituan_order_data.insert(4, "order_day_time", order_day_time_all)

    return meituan_order_data

def get_customer_activity(meituan_order_data):
    '''
    获取活跃用户的数据，近一个月有下单记录的
    :param meituan_order_data:
    :return:
    '''
    customer_activity = meituan_order_data[meituan_order_data['days_differ_count'] > -30]
    customer_activity = customer_activity.groupby('phone').size()
    customer_activity.to_csv("meituan_customer_activity.csv", index=True, sep=',')

def get_customer_sleep(meituan_order_data):
    '''
    获取沉睡用户的数据，近1-2个月有下单记录的客户
    :param meituan_order_data:
    :return:
    '''
    customer_sleep = meituan_order_data[meituan_order_data['days_differ_count'] < -30]
    customer_sleep = customer_sleep[customer_sleep['days_differ_count'] > -60]
    customer_sleep = customer_sleep.groupby('phone').size()
    customer_sleep.to_csv("meituan_customer_sleep.csv", index=True, sep=',')

def get_customer_go_away(meituan_order_data):
    '''
    获取活跃用户的数据，近一个月有下单记录的
    :param meituan_order_data:
    :return:
    '''
    customer_go_away = meituan_order_data[meituan_order_data['days_differ_count'] < -60]
    customer_go_away = customer_go_away.groupby('phone').size()
    customer_go_away.to_csv("meituan_customer_go_away.csv", index=True, sep=',')


def get_workday_prefer_customer(meituan_order_data):
    '''
    获取工作日偏好的用户数据   订单在周一到周五
    :param meituan_order_data:
    :return:
    '''
    workday_prefer_customer = meituan_order_data[meituan_order_data['order_weekday']<6]
    workday_prefer_customer = workday_prefer_customer.groupby('phone').size()
    workday_prefer_customer.to_csv("meituan_workday_prefer_customer.csv", index=True, sep=',')

def get_weekday_prefer_customer(meituan_order_data):
    '''
    获取周末偏好的用户数据   订单在周六周日
    :param meituan_order_data:
    :return:
    '''
    weekday_prefer_customer = meituan_order_data[meituan_order_data['order_weekday'] > 5]
    weekday_prefer_customer = weekday_prefer_customer.groupby('phone').size()
    weekday_prefer_customer.to_csv("meituan_weekday_prefer_customer.csv", index=True, sep=',')


def get_breakfast_prefer_customer(meituan_order_data):
    '''
    获取午餐偏好的用户的数据    订单时间段在午餐时间段
    :param meituan_order_data:
    :return:
    '''
    breakfast_prefer_customer = meituan_order_data[meituan_order_data['order_day_time'] == 1]
    breakfast_prefer_customer = breakfast_prefer_customer.groupby('phone').size()
    breakfast_prefer_customer.to_csv("meituan_breakfast_prefer_customer.csv", index=True, sep=',')

def get_dinner_prefer_customer(meituan_order_data):
    '''
    获取晚餐偏好用户的数据   订单时间段在晚餐时间段
    :return:
    '''
    dinner_prefer_customer = meituan_order_data[meituan_order_data['order_day_time'] == 2]
    dinner_prefer_customer = dinner_prefer_customer.groupby('phone').size()
    dinner_prefer_customer.to_csv("meituan_dinner_prefer_customer.csv", index=True, sep=',')

def get_night_prefer_customer(meituan_order_data):
    '''
    获取宵夜偏好的用户的数据  订单时间段在宵夜时间段
    :return:
    '''
    night_prefer_customer = meituan_order_data[meituan_order_data['order_day_time'] == 3]
    night_prefer_customer = night_prefer_customer.groupby('phone').size()
    night_prefer_customer.to_csv("meituan_night_prefer_customer.csv", index=True, sep=',')


if __name__  == '__main__':
    meituan_order_data = get_time_differ(meituan_order_data)

    get_customer_activity(meituan_order_data)
    get_customer_sleep(meituan_order_data)
    get_customer_go_away(meituan_order_data)

    get_workday_prefer_customer(meituan_order_data)
    get_weekday_prefer_customer(meituan_order_data)

    get_breakfast_prefer_customer(meituan_order_data)
    get_dinner_prefer_customer(meituan_order_data)
    get_night_prefer_customer(meituan_order_data)


