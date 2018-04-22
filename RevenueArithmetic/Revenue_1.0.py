#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: Ryan
# @Time: 2018/1/26 下午4:32


'''
营收算法1.0，按照权重来算满减金额，获取商家一个月的预计营收，效果不太好，可能时数据太少
'''
from util.DB.DAO import DBUtils,BatchSql
db = DBUtils(('192.168.1.200', 3306, 'njjs', 'njjs1234', 'exdata_2018', 'utf8mb4'))

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


def getMinIndex(my_list):
    min = my_list[0]
    for i in my_list:
        if i < min:
            min = i
    return my_list.index(min)

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
            index_price = []
            jian_price = []


            for (index,sub_price) in shop_money_off.items():
                index_price.append(index)
                jian_price.append(sub_price)


            for i in range(len(index_price)):
                if item[3]>index_price[i]:
                    mid_line = i
                    break

            for (index,sub_price) in shop_money_off.items():
                if (item[3]>=30 and item[3]<=40):
                    big_section_weight = abs(item[3] - index_price[mid_line - 1]) / (
                            index_price[mid_line - 1] - index_price[mid_line])
                    small_section_weight = abs(item[3] - index_price[mid_line]) / (
                            index_price[mid_line - 1] - index_price[mid_line])
                    ture_sub_price = jian_price[mid_line - 1] * big_section_weight * 1.05 + jian_price[
                        mid_line] * small_section_weight*1.01


                    all_common_menu_amount += item[2]*(item[3]-abs(ture_sub_price)+2)
                    print('权重数据：',item[2],item[3], abs(ture_sub_price))
                    break

                elif item[3] > index-10:
                    all_common_menu_amount += item[2]*(item[3]-abs(sub_price)+2)
                    print('直接满减：',item[2],item[3], abs(sub_price))
                    break
                elif (item[3]>=60):
                    all_common_menu_amount += item[2] * (item[3] - abs(sub_price)+2)
                    print('直接满减：', item[2], item[3], abs(sub_price))
                    break



    print("普通订单总金额：",all_common_menu_amount)
    return all_common_menu_amount





if __name__ == '__main__':
    shopid = [160279990,161341608,161378073,161313783]

    for item in shopid:
        print("店铺%s的1月份营收总金额为："%item)
        shop_money_off = get_shop_money_off(item)
        shop_menu_order = get_shop_menu_order(item)
        all_single_menu_amount = deal_single_menu(shop_menu_order)
        all_activity_menu_amount = deal_activity_menu(shop_menu_order)
        all_common_menu_amount = deal_common_menu(shop_money_off,shop_menu_order)

        all_amount = all_single_menu_amount+all_activity_menu_amount+all_common_menu_amount
        print("营收总金额：",all_amount)
        print("################################")
