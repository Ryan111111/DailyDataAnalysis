#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: Ryan
# @Time: 2018/1/29 下午2:09

'''
处理外卖大师web端的店圈监控的数据，分别获取该店圈的1.5,3,5公里范围的所有店铺数据
'''

from util.DB.DAO import DBUtils,BatchSql
import datetime
import json

'''
数据库信息，数据库1：web端用来保存外卖大师的数据
          数据库2：查询城市数据
'''

db1 = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'wmds', 'utf8mb4'))
db2 = DBUtils(('192.168.1.200', 3306, 'njjs', 'njjs1234', 'exdata_2018', 'utf8mb4'))
time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


'''
获取配置信息：店铺名称，店铺id，店铺经纬度，店圈范围半径类型
'''
area_shop_name = ['谢恒兴奇味鸡煲(义乌店)','谢恒兴奇味鸡煲(河西万达店)','谢恒兴奇味鸡煲(同曦鸣城店)','谢恒兴奇味鸡煲(殷巷店)','谢恒兴奇味鸡煲(明发广场店)','谢恒兴奇味鸡煲(小市店)','谢恒兴奇味鸡煲(油坊桥店)','谢恒兴奇味鸡煲(元通店)','奇味鸡煲(四方新村店)']
area_shop_id = [160279990,161289358,161378073,161313783,161341608,156753255,4166456,161507150,161506911]
shop_latitude = [31.937813,32.03445,31.9427300,31.9099300,31.9776340,32.0933240,31.9664130,31.992613,32.0185100]
shop_longitude = [118.8760630,118.74473,118.8231500,118.8357800,118.7976980,118.7895010,118.7213890,118.709933,118.8420300]
type = [1500,3000,5000]


def deal_quan_position():
    '''
    处理店圈的店铺列表信息，获取1.5,3,5公里的店圈id列表信息，并写入数据表quan_position中
    :return:
    '''
    for i in range(len(area_shop_id)):          #遍历所有店铺列表
        for j in range(0,3):                    #获取三种店圈半径类型的数据
            sql = """
                  SELECT t1.rest_id
                    FROM t_e_rest_list_city_1712 t1
                    where t1.longitude IN
                    (SELECT t1.longitude
                     FROM
                     t_e_rest_list_city_1712 t1
                    WHERE
                     t1.city = 'nanjing'
                    AND round(
                     6378.138 * 2 * asin(
                      sqrt(
                       pow(
                        sin(
                         (
                          t1.latitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       ) + cos(t1.latitude * pi() / 180) * cos(%s * pi() / 180) * pow(
                        sin(
                         (
                          t1.longitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       )
                      )
                     ) * 1000
                    ) <= %s)
                    and t1.latitude IN
                    (SELECT t1.latitude
                     FROM
                     t_e_rest_list_city_1712 t1
                    WHERE
                     t1.city = 'nanjing'
                    AND round(
                     6378.138 * 2 * asin(
                      sqrt(
                       pow(
                        sin(
                         (
                          t1.latitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       ) + cos(t1.latitude * pi() / 180) * cos(%s * pi() / 180) * pow(
                        sin(
                         (
                          t1.longitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       )
                      )
                     ) * 1000
                    ) <= %s)
            
                  """%(shop_latitude[i],shop_latitude[i],shop_longitude[i],type[j],shop_latitude[i],shop_latitude[i],shop_longitude[i],type[j])
            # print(sql)
            area_shop_list = db2.queryForList(sql,None)   #获取店圈的店铺id信息
            all_area_shop_list = []
            for item in area_shop_list:
                all_area_shop_list.append(item[0])

            all_str_shop_ids = str(all_area_shop_list)          #店铺列表信息的简单处理，将[]去除
            all_str_shop_ids = all_str_shop_ids.split('[')[1]
            all_str_shop_ids = all_str_shop_ids.split(']')[0]

            sql2 = 'insert into quan_position values'
            quan_position = []
            quan_position.extend([0,'2017-12-31',area_shop_id[i],area_shop_name[i],j+1,all_str_shop_ids,time,time])
            print("店圈列表信息：",quan_position)

            batch = BatchSql(sql2)
            batch.addBatch(quan_position)
            db1.update(batch)


def get_all_shopid_list():
    '''
    获取所有店圈去重后的的店铺id，
    :return:
    '''
    unique_area_shop_list = set()
    for i in range(len(area_shop_id)):
        for j in range(0, 3):
            sql = """
                  SELECT t1.rest_id
                    FROM t_e_rest_list_city_1712 t1
                    where t1.longitude IN
                    (SELECT t1.longitude
                     FROM
                     t_e_rest_list_city_1712 t1
                    WHERE
                     t1.city = 'nanjing'
                    AND round(
                     6378.138 * 2 * asin(
                      sqrt(
                       pow(
                        sin(
                         (
                          t1.latitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       ) + cos(t1.latitude * pi() / 180) * cos(%s * pi() / 180) * pow(
                        sin(
                         (
                          t1.longitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       )
                      )
                     ) * 1000
                    ) <= %s)
                    and t1.latitude IN
                    (SELECT t1.latitude
                     FROM
                     t_e_rest_list_city_1712 t1
                    WHERE
                     t1.city = 'nanjing'
                    AND round(
                     6378.138 * 2 * asin(
                      sqrt(
                       pow(
                        sin(
                         (
                          t1.latitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       ) + cos(t1.latitude * pi() / 180) * cos(%s * pi() / 180) * pow(
                        sin(
                         (
                          t1.longitude * pi() / 180 - %s * pi() / 180
                         ) / 2
                        ),
                        2
                       )
                      )
                     ) * 1000
                    ) <= %s)

                  """ % (
            shop_latitude[i], shop_latitude[i], shop_longitude[i], type[j], shop_latitude[i], shop_latitude[i],
            shop_longitude[i], type[j])
            area_shop_list = db2.queryForList(sql, None)  # 获取店圈的店铺id信息
            all_area_shop_list = []
            for item in area_shop_list:
                all_area_shop_list.append(item[0])
            print(all_area_shop_list)
            unique_area_shop_list.update(all_area_shop_list)
    return unique_area_shop_list


def deal_quan_shop(city,date,all_shopid_list,cate_list):
    '''
    处理店圈店铺的基本数据
    :param city:  店圈所属城市
    :param date:  数据分析日期
    :param all_shopid_list:  店圈数据的店铺列表的数据
    :param cate_list: 店圈数据的品类列表
    :return:
    '''
    sql = """
          select t1.rest_id,t1.rest_name,t1.delivery_id,t1.longitude,t1.latitude,t1.is_new
          from t_e_rest_list_city_1712 t1
          where t1.city = '%s'
          and t1.date = '%s'
          and t1.rest_id IN %s
          """%(city,date,all_shopid_list)

    sql2 = "insert into quan_shop values"
    batch = BatchSql(sql2)

    result = db2.queryForList(sql, None)
    print(result)
    for item in result:
        for cate in cate_list:
            if item[0] == cate[0]:
                quan_shop = []
                quan_shop.extend([0,item[0],1,date,item[1],item[2],item[3],item[4],item[5],cate[1],time,time])
                print(quan_shop)
                batch.addBatch(quan_shop)

        if batch.getSize() > 1000:
            db1.update(batch)
            batch.cleanBatch()
    db1.update(batch)


def deal_quan_monitor_shop_data(city,date,all_shopid_list):
    '''
    处理店圈的配送费用和配送时间的信息
    :param city: 店圈所属城市
    :param date: 爬取数据的日期
    :param all_shopid_list: 所有店圈的店铺列表信息
    :return:
    '''
    sql = """
          select t1.rest_id,t1.order_month_sales,t2.deliver_time,t1.delivery_fee,t1.min_delivery_price,t2.overall_score
          from t_e_rest_list_city_1712 t1,
               t_e_rest_score_city_1712 t2
          where t1.city  = '%s'
          and t1.date = '%s'
          and t1.rest_id IN %s
          and t1.rest_id = t2.rest_id
          and t1.city = t2.city
          and t1.date = t2.date
          """%(city,date,all_shopid_list)

    sql2 = "insert into quan_monitor_shop_data values"
    batch = BatchSql(sql2)

    result = db2.queryForList(sql, None)

    for item in result:
        quan_monitor_shop_data = []
        score = float('%.1f'%float(item[5]))
        quan_monitor_shop_data.extend([0,date,item[0],item[1],item[2],item[3],item[4],score,time,time])
        print(quan_monitor_shop_data)
        batch.addBatch(quan_monitor_shop_data)

        if batch.getSize() > 10000:
            db1.update(batch)
            batch.cleanBatch()
    db1.update(batch)



def get_sub_price(city,date,shopid,price):
    '''
    处理sku满减的数据
    :param city: 店圈数据的城市
    :param date: 爬取数据的日期
    :param shopid: 该sku数据所属的店铺id
    :param price: 该sku的原价
    :return: 该sku满减活动优惠的金额
    '''
    sql = """
               select max(t1.sub_price)
               from t_e_rest_money_off_city_1712 t1
               where t1.city = '%s'
                and t1.date = '%s'
                and t1.full_price < %s
                and t1.rest_id = %s
          """%(city,date,price,shopid)
    result = db2.queryForList(sql, None)
    return result[0][0]


def deal_quan_monitor_hot_sku_data(city,date,all_shopid_list):
    '''
    处理店圈sku数据，9个店铺大约110万条数据
    :param city: 所属城市
    :param date: 数据日期
    :param all_shopid_list: 店圈店铺ID列表
    :return:
    '''
    sql="""
        select t1.rest_id,t1.food_name,t1.price,t1.food_id,t1.food_month_sales,t1.has_activity,t2.delivery_fee
        from t_e_rest_menu_level2_unique_city_nj_1712 t1,
             t_e_rest_list_city_1712 t2   
        where t1.city = '%s'
        and t1.date = '%s'
        and t1.rest_id IN %s
        and t1.food_month_sales != 0
        and t1.city = t2.city
        and t1.date = t2.date
        and t1.rest_id = t2.rest_id
        """%(city,date,all_shopid_list)
    sql2 = 'insert into quan_monitor_hot_sku_data values'
    batch = BatchSql(sql2)
    result = db2.queryForList(sql, None) 
    print(result)

    for shop_list in all_shopid_list:
        for item in result:
            quan_monitor_hot_sku_data = []
            if shop_list == item[0]:
                if item[5] == 1:   #特价菜
                    quan_monitor_hot_sku_data.extend([0,date,shop_list,item[1],item[2],item[2],item[4],time,time])
                    print(quan_monitor_hot_sku_data)
                    batch.addBatch(quan_monitor_hot_sku_data)
                elif item[5] == 0:  #满减菜品
                    sub_price = get_sub_price(city,date,shop_list,item[2])
                    if sub_price == None:
                        sub_price = 0
                    true_price = item[2]-sub_price
                    quan_monitor_hot_sku_data.extend([0,date,shop_list,item[1],item[2],true_price,item[4],time,time])
                    print(quan_monitor_hot_sku_data)
                    batch.addBatch(quan_monitor_hot_sku_data)
        if batch.getSize() > 10000:
            db1.update(batch)
            batch.cleanBatch()
    db1.update(batch)



def deal_quan_monitor_activity_data(city,date,all_shopid_list):
    '''
    处理店圈店铺的活动信息
    :param city: 所属城市
    :param date: 数据日期
    :param all_shopid_list: 店圈店铺的ID列表
    :return:
    '''
    sql="""
        select t1.rest_id,t1.description,t1.active_type
        from t_e_rest_active_city_1712 t1
        where t1.city  = '%s'
        and t1.date = '%s'
        and t1.rest_id IN %s
        """%(city,date,all_shopid_list)
    sql2 = "insert into quan_monitor_activity_data values"
    batch = BatchSql(sql2)
    result = db2.queryForList(sql, None)
    print(result)

    for shoplist in all_shopid_list:
        all_activity = []
        quan_monitor_activity_data = []
        for item in result:
            activity = dict(type=item[2],content=item[1])
            if item[0] == shoplist:
                all_activity.append(activity)
        all_activity = json.dumps(all_activity)
        quan_monitor_activity_data.extend([0,date,shoplist,str(all_activity),time,time])
        print(quan_monitor_activity_data)
        batch.addBatch(quan_monitor_activity_data)
        if batch.getSize() > 10000:
            db1.update(batch)
            batch.cleanBatch()
    db1.update(batch)

def deal_quan_category(city,date):
    '''
    处理店圈的菜品类信息，一级品类，二级品类信息
    :param city:
    :param date:
    :return:
    '''
    sql = """
        select DISTINCT(t1.category_id_level1),t1.category_name_level1,0 pid         
        from t_e_rest_category_city_1712 t1
        where t1.city = '%s'
        and t1.date = '%s'
        union 
        select DISTINCT(t1.category_id_level2),t1.category_name_level2,t1.category_id_level1 pid         
        from t_e_rest_category_city_1712 t1
        where t1.city = '%s'
        and t1.date = '%s'
        
          """%(city,date,city,date)
    result = db2.queryForList(sql, None)
    print(result)

    sql2 = "insert into quan_category values"
    batch = BatchSql(sql2)

    for item in result:
        quan_category = []
        quan_category.extend([0,item[1],date,item[0],item[2],time,time])
        print(quan_category)
        batch.addBatch(quan_category)
        if batch.getSize() > 10000:
            db1.update(batch)
            batch.cleanBatch()
    db1.update(batch)


def deal_quan_cate_shop(city,date,all_shopid_list):
    '''
    处理店圈的菜品信息
    :param city:
    :param date:
    :param all_shopid_list:
    :return: 菜品分类与店铺ID的关联信息
    '''
    sql="""
        select t1.category_id_level2,t1.rest_id
        from t_e_rest_category_city_1712 t1
        where t1.city = '%s'
        and t1.date = '%s' 
        """%(city,date)
    result = db2.queryForList(sql, None)
    print(result)

    all_cate_data = []
    for shoplist in all_shopid_list:
        category_id = []
        quan_cate_shop = []
        for item in result:
            if item[1] == shoplist:
                category_id.append(item[0])

        category_id = str(category_id)
        category_id = category_id.split('[')[1]
        category_id = category_id.split(']')[0]

        quan_cate_shop.extend([shoplist,category_id])
        all_cate_data.append(quan_cate_shop)

    return all_cate_data


if __name__ == '__main__':

    # all_shopid_list = {152174595,156893196}
    # all_shopid_list = {152174595, 156893196, 157679629, 152174616, 262171}
    all_shopid_list = get_all_shopid_list()
    all_str_shop_ids = tuple(all_shopid_list)  #对店铺列表进行去重


    deal_quan_position()
    deal_quan_monitor_shop_data('nanjing','2017-12-31',all_str_shop_ids)
    deal_quan_monitor_activity_data('nanjing','2017-12-31',all_str_shop_ids)
    deal_quan_category('nanjing','2017-12-31')

    cate_list = deal_quan_cate_shop('nanjing','2017-12-31',all_str_shop_ids)
    deal_quan_shop('nanjing','2017-12-31',all_str_shop_ids,cate_list)
    deal_quan_monitor_hot_sku_data('nanjing','2017-12-31',all_str_shop_ids)