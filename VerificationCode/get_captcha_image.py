import requests
'''
爬虫获取图片数据源
'''
for i in range(9130,10500):
    url = "https://verify.meituan.com/v2/captcha?action=spiderindefence&request_code=ee45fb4d58de4e46b530b35bdbdc3da8"
    html = requests.get(url)
    save_path = 'D:\\pic\\' + str(i) +'.jpg'
    with open(save_path, 'wb') as file:
        file.write(html.content)