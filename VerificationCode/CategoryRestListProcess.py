from util.proxies.ProxiesServer import ProxiesServer
import requests
import json
import time
import random
import traceback

from util.token.TokenUtil import TokenUtil
from concurrent.futures import ThreadPoolExecutor

class CategoryShopListProcess(object):

    def __init__(self,tokenUtil,lat, lng):
        self.tokenUtil = tokenUtil
        self.lat = lat
        self.lng = lng
        self.str_lat = str(int(round(float(self.lat), 6) * 1000000))
        self.str_lng = str(int(round(float(self.lng), 6) * 1000000))

        # self.cookies = None
        # self.rest_cookie = None
        self.initSession()

    def initSession(self):
        home_url = 'http://i.waimai.meituan.com/home'
        headers = {
            'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'cache-control': "max-age=0",
            'connection': "keep-alive",
            'cookie': "_lxsdk_s=%7C%7C0",
            'host': "i.waimai.meituan.com",
            'referer': "http//i.waimai.meituan.com/city",
            'upgrade-insecure-requests': "1",
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1"
        }

        self.session = requests.Session()
        query_params = {
            'lat': self.lat,
            'lng': self.lng
        }
        print("初始化session爬取经纬度:", query_params)
        initial_response = None
        while not initial_response:
            try:
                initial_response = self.session.request(
                    method='get',
                    url=home_url,
                    params=query_params,
                    headers=headers,
                    timeout=10
                )
                # print(initial_response.text)
                # self.cookies = initial_response.cookies
            except Exception as e:
                print("初始化session报错", e)
                pass
            time.sleep(1)

    def validate(self,data):
        flag = False
        try:
            if data['code'] == 0:
                flag = True
            else:
                print('被封休息2秒',data)
                time.sleep(2)
        except:
            print("判断是否被封报错", data)
        return flag

    def proxiesPostByToken(self,format_url,encrypt_url,headers=None,data=None):
        for i in range(20):
            print("第", i, "次请求", encrypt_url)
            token = self.tokenUtil.getToken(encrypt_url)
            # print("token", token)
            url = format_url.format(token)
            try:
                response = self.session.post(
                    url,
                    headers=headers,
                    data=data,
                    cookies = self.session.cookies,
                    timeout=10
                )
                # print("请求返回数据", response.text)
                result = json.loads(response.text)
                flag = self.validate(result)
                if flag:
                    return result
            except Exception as e:
                # print("获取数据报错",traceback.print_exc())
                print("获取数据报错",e)
        else:
            print("请求超过限制，判定没有数据", encrypt_url)
            return []

    def proxiesPost(self,url,headers=None,data=None,params=None):
        """
            通过代理获取数据
        :param url:
        :param data:
        :param header:
        :return:
        """
        i = 0
        while True:
            i += 1
            print("第",i,"次请求",url)
            if i>20:
                print("请求异常超出次数:",url)
                return []
            try:
                response = self.session.post(
                    url,
                    headers=headers,
                    data=data,
                    params = params,
                    cookies = self.session.cookies,
                    timeout=10
                )
                result = json.loads(response.text)
                # print("正常返回数据",result)
                if self.validate(result):
                    return result
            except Exception as e:
                print("获取数据报错",e)

    def getHasCountCategoryList(self):
        """
            获取有数量的品类
        :return:
        """
        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "75",
            'content-type': "application/x-www-form-urlencoded",
            'host': "i.waimai.meituan.com",
            'origin': "http//i.waimai.meituan.com",
            'referer': "http://i.waimai.meituan.com/channel?category_type=101065&category_text=%E7%BE%8E%E9%A3%9F",
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1",
            'x-requested-with': "XMLHttpRequest"
        }

        data = {
            'navigate_type': 101065,
            'first_category_type': 101065,
            'second_category_type': 0
        }

        url = "http://i.waimai.meituan.com/ajax/v6/poi/getfilterconditions"
        print("获取品类数据", url)

        result = self.proxiesPost(url,headers=headers, data=data)
        data = result.get('data').get('category_filter_list')
        category_list = []
        for item in data[1:]:
            category_id_level1 = item.get('code')
            category_name_level1 = item.get('name')
            for item2 in item.get('sub_category_list')[1:]:
                count = int(item2.get('quantity'))
                if count > 0:
                    category_id_level2 = item2.get('code')
                    category_name_level2 = item2.get('name')
                    category_list.append(
                        {'category_id_level1': category_id_level1, 'category_name_level1': category_name_level1,
                         'category_id_level2': category_id_level2, 'category_name_level2': category_name_level2,
                         'count': count})
        return category_list

    def getCategoryShopList(self, category_type, second_category_type):
        """
            根据品类获取店铺列表信息
        :param category_level1:
        :param category_level2:
        :return:
        """
        i = 0
        while True:
            try:
                print(category_type, "=", second_category_type, "第", i, "页")
                result = self.getCategoryShopPage(category_type, second_category_type, i)
                time.sleep(random.randint(0,5))
                print("获取数据结束")
                for shop in result['data']['poilist']:
                    yield shop
                if not result['data']['poi_has_next_page']:
                    break
                i += 1
            except:
                return []

    def getCategoryShopPage(self, category_type, second_category_type, index=0):
        """
            获取某个品类下店铺某页的店铺列表信息
        :param category_level1: 一级品类名称
        :param category_level2: 二级品类名称
        :param index: 页数
        :return:
        """
        headers = {
            'accept': "application/json",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "111",
            'content-type': "application/x-www-form-urlencoded",
            'host': "i.waimai.meituan.com",
            'origin': "http//i.waimai.meituan.com",
            'referer': "http//i.waimai.meituan.com/channel?category_type=101085&second_category_type=101119",
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1",
            'x-requested-with': "XMLHttpRequest"
        }

        data = {
            'uuid': self.session.cookies.get('w_uuid'),
            'platform': '3',
            'partner': '4',
            'page_index': '%s' % index,
            'apage': '1'
        }

        format_url = "http://i.waimai.meituan.com/ajax/v6/poi/filter?category_type=%s&second_category_type=%s" % (
            category_type, second_category_type) + "&_token={}"

        encrypt_url = '/ajax/v6/poi/filter?category_type=%s&second_category_type=%s&page_index=%s&apage=1' % (
            category_type, second_category_type, index)
        result = self.proxiesPostByToken(format_url, encrypt_url,headers=headers, data=data)
        if result:
            return result

    def getFood(self,rest_id):

        rest_id = '557620889835558'
        # self.initRestSession(rest_id)
        # self.initApi()

        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "116",
            'content-type': "application/x-www-form-urlencoded",
            'cookie': 'w_visitid=d438ac6c-c80e-4253-af2c-d6a4b9af9d5c; webp=1; _lxsdk_cuid=1600045f4dbc8-02ded62aa661e6-574e6e46-3d10d-1600045f4db3a; _lxsdk=1600045f4dbc8-02ded62aa661e6-574e6e46-3d10d-1600045f4db3a; utm_source=0; w_cid=320100; w_cpy_cn="%E5%8D%97%E4%BA%AC"; w_cpy=nanjing; w_latlng=32060254,118796877; __mta=208467629.1511833074504.1511833084989.1511833098081.3; _lxsdk_s=1600045f4dd-837-59e-97d%7C%7C6; terminal=i; w_utmz="utm_campaign=(direct)&utm_source=5000&utm_medium=(none)&utm_content=(none)&utm_term=(none)"; w_uuid=2UAHG4TZSyHfEomBhtI0LnjPRS2O1RD9cOtAoJUZbfBO5UBuKOFkEVE41Lbr6v59; wm_poi_view_id=557620889835558; poiid=557620889835558; JSESSIONID=kernx9ukwwf314rc2xsq14jof',
            'host': "i.waimai.meituan.com",
            'origin': "http//i.waimai.meituan.com",
            'referer': "http//i.waimai.meituan.com/restaurant/296920669848537",
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1",
            'x-requested-with': "XMLHttpRequest"
        }

        data = {
            'wm_poi_id': str(rest_id),
            'uuid': '2UAHG4TZSyHfEomBhtI0LnjPRS2O1RD9cOtAoJUZbfBO5UBuKOFkEVE41Lbr6v59',
            'platform': '3',
            'partner': '4',
        }

        format_url = 'http://i.waimai.meituan.com/ajax/v8/poi/food?_token=%s'
        encrypt_url = "/ajax/v8/poi/food?wm_poi_id=%s" %  rest_id
        print("encrypt_url:", encrypt_url)

        result = self.proxiesPostByToken(format_url, encrypt_url, headers=headers, data=data)
        if result:
            print("food", result)

    def initRestSession(self,rest_id):

        home_url = 'http://i.waimai.meituan.com/restaurant/%s' % rest_id
        headers = {
            'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'host': "i.waimai.meituan.com",
            'referer': "http//i.waimai.meituan.com/home?lat=32.060254&lng=118.796877",
            'upgrade-insecure-requests': "1",
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1",
        }

        initial_response = None
        while not initial_response:
            try:
                initial_response = self.session.request(
                    method='get',
                    url=home_url,
                    headers=headers,
                    cookies=self.session.cookies,
                    timeout=10
                )
                print("返回状态码：",initial_response.status_code)
            except Exception as e:
                print("初始化session报错", e)
                pass

    def initApi(self):
        url = 'http://default.queit.in/api'

        payload = "{\"type\":\"report\",\"apiKey\":\"dp_wai\",\"sessionId\":\"wi2e4lsyqdef15pym1dj6gm81\",\"metadata\":{\"pageStructure\":{\"fieldsList\":[],\"elementsList\":[]},\"reportInfo\":{\"id\":\"4598231330\",\"num\":1,\"time\":877},\"version\":\"4.2.17\",\"extra\":{},\"iframe\":false,\"fp\":{\"hash\":\"c487a089b1b7164c497a7c6c5ca430e8\",\"ub84\":[],\"ub82\":[\"Arial\",\"Arial Black\",\"Arial Narrow\",\"Arial Unicode MS\",\"Book Antiqua\",\"Bookman Old Style\",\"Calibri\",\"Cambria\",\"Cambria Math\",\"Century\",\"Century Gothic\",\"Century Schoolbook\",\"Comic Sans MS\",\"Consolas\",\"Courier\",\"Courier New\",\"Garamond\",\"Georgia\",\"Helvetica\",\"Impact\",\"Lucida Bright\",\"Lucida Calligraphy\",\"Lucida Console\",\"Lucida Fax\",\"Lucida Handwriting\",\"Lucida Sans\",\"Lucida Sans Typewriter\",\"Lucida Sans Unicode\",\"Microsoft Sans Serif\",\"Monotype Corsiva\",\"MS Gothic\",\"MS PGothic\",\"MS Reference Sans Serif\",\"MS Sans Serif\",\"MS Serif\",\"Palatino Linotype\",\"Segoe Print\",\"Segoe Script\",\"Segoe UI\",\"Segoe UI Light\",\"Segoe UI Semibold\",\"Segoe UI Symbol\",\"Tahoma\",\"Times\",\"Times New Roman\",\"Trebuchet MS\",\"Verdana\",\"Wingdings\",\"Wingdings 2\",\"Wingdings 3\"]},\"reportReason\":[\"new_data\"]},\"data\":{\"events\":[]}}"

        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "4877",
            'content-type': "text/plain;charset=UTF-8",
            'host': "default.queit.in",
            'origin': "http//i.waimai.meituan.com",
            'referer': "http//i.waimai.meituan.com/restaurant/296920669848537",
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1"
        }

        response = self.session.post(
            url=url,
            headers=headers,
            data=payload,
        )

        print("api返回结果：",response.status_code)

    def getShopInfo(self, poiid):
        """
            获取指定店铺的店铺信息
        :param poiid:
        :return:
        """
        self.initRestSession(poiid)
        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "114",
            'content-type': "application/x-www-form-urlencoded",
            'host': "i.waimai.meituan.com",
            'origin': "http//i.waimai.meituan.com",
            'referer': "http//i.waimai.meituan.com/restaurant/%s" % poiid,
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1",
            'x-requested-with': "XMLHttpRequest"
        }

        data = {
            'wmpoiid': poiid,
            'uuid': self.session.cookies.get('w_uuid'),
            'platform': 3,
            'partner': 4
        }

        format_url = 'http://i.waimai.meituan.com/ajax/v6/poi/info?_token=%s'
        # print("comment:", format_url % poiid)

        encrypt_url = "/ajax/v6/poi/info?wmpoiid=%s" % poiid

        result = self.proxiesPostByToken(format_url, encrypt_url, headers=headers, data=data)
        if result:
            # self.putIntoQueue('food',poiid,result)
            print("shop_info", result)

    def getCommentPage(self,poiid,index=0):

        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "187",
            'content-type': "application/x-www-form-urlencoded",
            'host': "i.waimai.meituan.com",
            'origin': "http//i.waimai.meituan.com",
            'referer': "http//i.waimai.meituan.com/restaurant/478163994198361",
            'user-agent': "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1",
            'x-requested-with': "XMLHttpRequest"
        }

        data = {
            'wmpoiid':poiid,
            'page_offset':0,
            'page_size':20,
            'comment_score_type':0,
            'filter_type':0,
            'label_id':0,
            'uuid':self.session.cookies.get('w_uuid'),
            'platform':3,
            'partner':4
        }

        format_url = 'http://i.waimai.meituan.com/ajax/poi/comment?_token=%s'
        encrypt_url = "/ajax/poi/comment?wmpoiid=%s" % poiid

        result = self.proxiesPostByToken(format_url, encrypt_url, headers=headers, data=data)
        if result:
            print("comment", result)


def crawler(t,lat,lng):
    print(lat,lng)
    with ThreadPoolExecutor(max_workers=1) as executor:
        manager = CategoryShopListProcess(t, lat, lng)
        category_list = manager.getHasCountCategoryList()
        for category in category_list:
            category_id_level1 = str(category.get('category_id_level1'))  # 一级品类id
            category_id_level2 = str(category.get('category_id_level2'))  # 二级品类id
            print("爬取品类：", category_id_level1 + "=" + category_id_level2)
            shops = manager.getCategoryShopList(category_id_level1,category_id_level2)
            for shop in shops:
                print(lat,"店铺",shop)
                mt_poi_id = str(shop.get('mt_poi_id'))
                rest_id = str(shop.get('id'))
                # executor.submit(manager.getFood, rest_id)
                # executor.submit(manager.getShopInfo, rest_id)
                # break
                # executor.submit(manager.getCommentPage, rest_id)
            break

# 被封需要验证码验证
# {'customData': {'verifyUrl': 'https://optimus-mtsi.meituan.com/optimus/verify?request_code=94e06b8043524989bdd5913bd8e7cf22', 'imageUrl': 'https://verify.meituan.com/v2/captcha?action=spiderindefence&request_code=94e06b8043524989bdd5913bd8e7cf22', 'verifyPageUrl': 'https://verify.meituan.com/v2/app/general_page?action=spiderindefence&requestCode=94e06b8043524989bdd5913bd8e7cf22&platform=3&succCallbackUrl=https://optimus-mtsi.meituan.com/optimus/verifyResult'}, 'code': 406, 'msg': '您的网络好像不太给力，请稍后再试'}
# {'customData': {'verifyUrl': 'https://optimus-mtsi.meituan.com/optimus/verify?request_code=05cdf553d88049979c84e12bd2ac937a', 'imageUrl': 'https://verify.meituan.com/v2/captcha?action=spiderindefence&request_code=05cdf553d88049979c84e12bd2ac937a', 'verifyPageUrl': 'https://verify.meituan.com/v2/app/general_page?action=spiderindefence&requestCode=05cdf553d88049979c84e12bd2ac937a&platform=3&succCallbackUrl=https://optimus-mtsi.meituan.com/optimus/verifyResult'}, 'code': 406, 'msg': '您的网络好像不太给力，请稍后再试'}

if __name__ == '__main__':
    t = TokenUtil()
    with ThreadPoolExecutor(max_workers= 1) as executor:
        with open("test.txt", 'r', encoding='utf-8') as f:
            for line in f:
                if line:
                    location = line.strip('\r').strip('\n').split(',')
                    executor.submit(crawler,t,location[1],location[0])