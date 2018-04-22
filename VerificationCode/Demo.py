# coding: utf-8
import requests
import json


def recogn(api_username, api_password, file_name, api_post_url, yzm_min, yzm_max, yzm_type, tools_token):
    '''
    api_username    （API账号）             --必须提供
    api_password    （API账号密码）         --必须提供
    file_name       （需要打码的图片路径）   --必须提供
    api_post_url    （API接口地址）         --必须提供
    yzm_min         （验证码最小值）        --可空提供
    yzm_max         （验证码最大值）        --可空提供
    yzm_type        （验证码类型）          --可空提供
    tools_token     （工具或软件token）     --可空提供
    '''
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0',
        # 'Content-Type': 'multipart/form-data; boundary=---------------------------227973204131376',
        'Connection': 'keep-alive',
        'Host': 'v1-http-api.jsdama.com',
        'Upgrade-Insecure-Requests': '1'
    }

    files = {
        'upload': (file_name, open(file_name, 'rb'), 'image/png')
    }

    data = {
        'user_name': api_username,
        'user_pw': api_password,
        'yzm_minlen': yzm_min,
        'yzm_maxlen': yzm_max,
        'yzmtype_mark': yzm_type,
        'zztool_token': tools_token
    }
    s = requests.session()
    r = s.post(api_post_url, headers=headers, data=data, files=files, verify=False)
    result = r.json()                       #将Textresponse对象转化成json
    print(r.json())
    result_recogn = result['data']['val']   #取出对应的验证码信息
    result_id = result['data']['id']        #取出识别的验证码ID

    print('识别验证码的结果：',result_recogn)
    print('识别验证码的ID：',result_id)

    return result,result_recogn,result_id


def error_report(api_username, api_password, yzm_id, api_post_url):
    '''
    判断错误向平台汇报
    :param api_username: 用户名
    :param api_password: 密码
    :param yzm_id: 识别的验证码ID
    :param api_post_url: 报错的借口地址
    :return:
    '''
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0',
        # 'Content-Type': 'multipart/form-data; boundary=---------------------------227973204131376',
        'Connection': 'keep-alive',
        'Host': 'v1-http-api.jsdama.com',
        'Upgrade-Insecure-Requests': '1'
    }

    data = {
        'user_name': api_username,
        'user_pw': api_password,
        'yzm_id': yzm_id
    }

    s = requests.session()
    r = s.post(api_post_url, headers=headers, data=data, verify=False)
    print('报错信息：',r.text)


def captcha_run(captcha_path,captcha_flag):
    '''
    验证码识别
    :param captcha_path: 验证码图片保存路径
    :captcha_flag: 验证码标志位
    :return:
    '''
    captcha_flag = True
    result, result_recogn, result_id = recogn('Ryan123',
                                              'NJzhishizhen123',
                                              captcha_path,
                                              "http://v1-http-api.jsdama.com/api.php?mod=php&act=upload",
                                              '',
                                              '',
                                              '1001',
                                              '')
    print("验证码列表结果：", result)
    print("验证码：", result_recogn)
    print("验证码返回ID：", result_id)

    if captcha_flag==True:   #验证码正确，
        pass
    elif captcha_flag==False: #如果验证码判断错误，执行报错程序
        error_report('Ryan123','NJzhishizhen123',result_id,'http://v1-http-api.jsdama.com/api.php?mod=php&act=error')

if __name__ == '__main__':
    captcha_path = 'C:/PIC/105.png'
    captcha_flag = True
    captcha_run(captcha_path,captcha_flag)




