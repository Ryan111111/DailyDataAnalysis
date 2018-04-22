#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: Ryan
# @Time: 2018/1/26 下午4:32


'''
营收算法2.0，按照顾客心理尽量取满减，获取商家一个月的预计营收，数据太少，需要检验
'''
from util.DB.DAO import DBUtils,BatchSql
from util.utilFunction import print_run_time
db = DBUtils(('192.168.1.200', 3306, 'njjs', 'njjs1234', 'exdata_2018', 'utf8mb4'))
sql = 'insert into shop_month_revenue values'
def get_shop_money_off(shopid):
    '''
    获取该商家的满减信息
    :return:
    '''
    sql = '''
          SELECT t1.rest_id,t1.full_price price,t1.sub_price
          FROM t_e_rest_money_off_city_1801 t1
          WHERE t1.rest_id = %s
          ORDER BY price DESC
          '''%(shopid)
    shop_money_off = db.queryForList(sql, None)

    shop_money_off_info = dict()
    for item in shop_money_off:
        shop_money_off_info[item[1]] = item[2]
    return shop_money_off_info

def get_shop_menu_order(shopid):
    '''
    获取商家菜品订单的数据
    :return:
    '''
    sql = '''
          SELECT t1.rest_id,t1.food_name,t1.food_month_sales,t1.price,t1.has_activity
          FROM t_e_rest_menu_level2_unique_city_nj_1801 t1
          WHERE t1.rest_id = %s
          '''%(shopid)
    shop_menu_order = db.queryForList(sql, None)
    return shop_menu_order

def deal_single_menu(shop_menu_order):
    '''
    处理单点不送的点单收入
    :param get_shop_menu_order:商家整个月菜品订单信息
    :return:该商店单点不送餐的总营收额
    '''
    all_single_menu_amount = 0
    for item in shop_menu_order:
        if item[3] <= 6:
            all_single_menu_amount +=item[2]*item[3]
    print("单点不送的点单收入:",all_single_menu_amount)
    return all_single_menu_amount

def deal_activity_menu(shop_menu_order):
    '''
    获取特价菜的订单总收入
    :param shop_menu_order:商家整个月菜品订单信息
    :return:特价菜的订单总收入
    '''
    all_activity_menu_amount = 0
    for item in shop_menu_order:
        if item[4] == 1:
            all_activity_menu_amount += item[2]*item[3]
    print("特价菜订单的总收入：",all_activity_menu_amount)
    return all_activity_menu_amount


def deal_common_menu(shop_money_off,shop_menu_order):
    '''
    处理普通的菜品订单，包含
    :param shop_money_off:
    :param shop_menu_order:
    :return:
    '''
    all_common_menu_amount = 0
    for item in shop_menu_order:
        if item[3] > 6 and item[4] == 0:
            for (index,sub_price) in shop_money_off.items():
                if item[3] > index-11:
                    all_common_menu_amount += item[2]*(item[3]-sub_price)
                    break
    print("普通订单总金额：",all_common_menu_amount)
    return all_common_menu_amount


def get_shopid():
    sql = '''
          SELECT t1.rest_id,t1.rest_name FROM t_e_rest_list_city_1801 t1
          WHERE t1.city = 'nanjing'
          '''
    shopid_list = db.queryForList(sql, None)
    rest_id = []
    rest_name = []
    for item in shopid_list:
        rest_id.append(item[0])
        rest_name.append(item[1])

    return rest_id,rest_name

@print_run_time
def run():
    # rest_id,rest_name = get_shopid()
    rest_name = ['义务','明发','同曦','殷象']
    rest_id = [160279990, 161341608, 161378073, 161313783]
    for item in rest_id:
        revenue = []
        print("店铺%s的12月份营收总金额为：" % item)
        shop_money_off = get_shop_money_off(item)
        shop_menu_order = get_shop_menu_order(item)
        all_single_menu_amount = deal_single_menu(shop_menu_order)
        all_activity_menu_amount = deal_activity_menu(shop_menu_order)
        all_common_menu_amount = deal_common_menu(shop_money_off, shop_menu_order)

        all_amount = all_single_menu_amount + all_activity_menu_amount + all_common_menu_amount
        print("营收总金额：", all_amount)
        revenue.extend([item,rest_name[rest_id.index(item)],'2017-12-31',all_amount])
        print(revenue)
        print("------------------------------")
        print(rest_id.index(item))

        batch = BatchSql(sql)
        batch.addBatch(revenue)
        db.update(batch)


if __name__ == '__main__':
    run()
    # get_shopid()