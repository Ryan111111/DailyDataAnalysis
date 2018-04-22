import numpy as np
import datetime
import pandas as pd


now_date = datetime.datetime.now().strftime('%Y-%m-%d')
now_date = datetime.datetime.strptime(now_date, "%Y-%m-%d")


filepath = 'elm_order_yiwu_20170123.csv'
order_elm_data = pd.read_csv(filepath,encoding='utf-8', header=None,skiprows=[0])
order_elm_data = pd.DataFrame(order_elm_data)
order_elm_data.columns = ['orderid','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','aa','ab','ac','ad','ae','af','ag']
order_elm_data = order_elm_data[order_elm_data['q'] != 'invalid']   #删除无效的订单


def get_days_differ(now_date,order_elm_data):
    '''
    获取订单时间与当前时间的差值
    :param now_date:
    :param order_elm_data:
    :return:
    '''
    days_differ_count = []
    order_weekdays_all = []
    order_day_time_all = []
    for i in range(0,len(order_elm_data)):
        order_time = order_elm_data.iloc[i,4]
        order_date = str(order_time).split('T')[0]

        order_date = datetime.datetime.strptime(order_date, "%Y-%m-%d")   #订单日期与当前时间之差
        days_differ = int((order_date-now_date).days)
        days_differ_count.append(days_differ)

        order_weekday = order_date.weekday() + 1                          #订单属于周几
        order_weekdays_all.append(order_weekday)

        order_day_time = str(order_time).split('T')[1]                    #订单时间段
        order_day_std_time = '00:00:00'
        order_day_seconds = (datetime.datetime.strptime(order_day_time,'%H:%M:%S')-datetime.datetime.strptime(order_day_std_time,'%H:%M:%S')).seconds
        if order_day_seconds > 36000 and order_day_seconds < 46800:
            order_day_time_all.append(1)       #午餐
        elif order_day_seconds > 59400 and order_day_seconds < 72000:
            order_day_time_all.append(2)       #晚餐
        elif (order_day_seconds > 75600 and order_day_seconds < 86399) or order_day_seconds < 21600:
            order_day_time_all.append(3)       #宵夜
        else:
            order_day_time_all.append(4)       #非指定时间段


    order_elm_data.insert(2,"days_differ_count",days_differ_count)
    order_elm_data.insert(3, "order_weekday", order_weekdays_all)
    order_elm_data.insert(4, "order_day_time", order_day_time_all)
    print(order_elm_data)
    return order_elm_data


def get_order_comment(order_elm_data):
    '''
    将评论数据与订单数据进行匹配
    :param order_elm_data:
    :return:含有评论的订单数据
    '''
    order_comment = pd.read_csv('elm_comment.csv', encoding='utf-8', header=None, skiprows=[0])
    order_comment = pd.DataFrame(order_comment)

    order_id = order_comment.iloc[:,2:4]
    order_id.columns = ['orderid','score']
    result = pd.merge(order_elm_data,order_id,how = 'left',on='orderid')
    return result

def get_order_good_comment_customer(result):
    '''
    获取好评用户的数据  评价分数为4，5星
    :param result:
    :return:
    '''
    good_comment_customer = result[result['score'] >= 4]
    good_comment_customer = good_comment_customer.groupby('o').size()
    good_comment_customer.to_csv("elm_good_comment_customer.csv", index=True, sep=',')


def get_order_bad_comment_customer(result):
    '''
    获取差评用户的数据  评价分数为1，2，3星
    :param result:
    :return:
    '''
    bad_comment_customer = result[result['score'] < 4]
    bad_comment_customer = bad_comment_customer.groupby('o').size()
    bad_comment_customer.to_csv("elm_bad_comment_customer.csv", index=True, sep=',')

def get_customer_activity(order_elm_data):
    '''
    获取活跃用户的数据：近一个月有下单记录的客户
    :param order_elm_data:
    :return:
    '''
    customer_activity = order_elm_data[order_elm_data['days_differ_count'] > -30]
    customer_activity = customer_activity.groupby('o').size()
    customer_activity.to_csv("elm_customer_activity.csv", index=True, sep=',')

def get_customer_sleep(order_elm_data):
    '''
    获取沉睡用户的数据，近1-2个月有下单记录的客户
    :param order_elm_data:
    :return:
    '''
    customer_sleep = order_elm_data[order_elm_data['days_differ_count'] < -30 ]
    customer_sleep = customer_sleep[customer_sleep['days_differ_count'] > -60 ]
    customer_sleep = customer_sleep.groupby('o').size()
    customer_sleep.to_csv("elm_customer_sleep.csv", index=True, sep=',')


def get_customer_go_away(order_elm_data):
    '''
    获取普通，高价值流失客户的数据：最后一次下单时间超过2个月
    :param order_elm_data:
    :return:
    '''
    customer_ordinary_away = order_elm_data[order_elm_data['days_differ_count'] < -60 ]
    customer_ordinary_away = customer_ordinary_away.groupby('o').size()
    customer_ordinary_away.to_csv("elm_customer_go_away.csv", index=True, sep=',')


def get_workday_prefer_customer(order_elm_data):
    '''
    获取工作日偏好的用户数据   订单在周一到周五
    :param order_elm_data:
    :return:
    '''
    workday_prefer_customer = order_elm_data[order_elm_data['order_weekday']<6]
    # print(workday_prefer_customer)
    # workday_prefer_customer = workday_prefer_customer.groupby(['o','order_weekday']).size()    按照两个列去数据
    workday_prefer_customer = workday_prefer_customer.groupby('o').size()
    workday_prefer_customer.to_csv("elm_workday_prefer_customer.csv", index=True, sep=',')

def get_weekday_prefer_customer(order_elm_data):
    '''
    获取周末偏好的用户数据   订单在周六周日
    :param order_elm_data:
    :return:
    '''
    weekday_prefer_customer = order_elm_data[order_elm_data['order_weekday'] > 5]
    weekday_prefer_customer = weekday_prefer_customer.groupby('o').size()
    weekday_prefer_customer.to_csv("elm_weekday_prefer_customer.csv", index=True, sep=',')
    # print(workday_prefer_customer)


def get_breakfast_prefer_customer(order_elm_data):
    '''
    获取午餐偏好的用户的数据    订单时间段在午餐时间段
    :param order_elm_data:
    :return:
    '''
    breakfast_prefer_customer = order_elm_data[order_elm_data['order_day_time'] == 1]
    breakfast_prefer_customer = breakfast_prefer_customer.groupby('o').size()
    breakfast_prefer_customer.to_csv("elm_breakfast_prefer_customer.csv", index=True, sep=',')


def get_dinner_prefer_customer(order_elm_data):
    '''
    获取晚餐偏好用户的数据   订单时间段在晚餐时间段
    :return:
    '''
    dinner_prefer_customer = order_elm_data[order_elm_data['order_day_time'] == 2]
    dinner_prefer_customer = dinner_prefer_customer.groupby('o').size()
    dinner_prefer_customer.to_csv("elm_dinner_prefer_customer.csv", index=True, sep=',')

def get_night_prefer_customer(order_elm_data):
    '''
    获取宵夜偏好的用户的数据  订单时间段在宵夜时间段
    :return:
    '''
    night_prefer_customer = order_elm_data[order_elm_data['order_day_time'] == 3]
    night_prefer_customer = night_prefer_customer.groupby('o').size()
    night_prefer_customer.to_csv("elm_night_prefer_customer.csv", index=True, sep=',')


if __name__ == '__main__':
    order_elm_data = get_days_differ(now_date,order_elm_data)
    result = get_order_comment(order_elm_data)                   #添加评价数据

    get_customer_activity(order_elm_data)
    get_customer_sleep(order_elm_data)
    get_customer_go_away(order_elm_data)

    get_workday_prefer_customer(order_elm_data)
    get_weekday_prefer_customer(order_elm_data)

    get_breakfast_prefer_customer(order_elm_data)
    get_dinner_prefer_customer(order_elm_data)
    get_night_prefer_customer(order_elm_data)

    get_order_good_comment_customer(result)
    get_order_bad_comment_customer(result)


