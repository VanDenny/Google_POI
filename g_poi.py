from Clawer_Base.clawer_frame import Clawer
from Clawer_Base.logger import logger
from Clawer_Base.geo_lab import Rectangle
from Clawer_Base.email_alerts import Email_alarm
from Clawer_Base.res_extractor import Res_Extractor
from Clawer_Base.key_changer import Key_Changer
from Clawer_Base.ioput import Res_saver, Type_Input
import pandas as pd
import os
import datetime
from multiprocessing.dummy import Pool as ThreadPool
import prettytable


class Params(dict):
    def __init__(self,a_dict):
        super(Params,self).update(a_dict)

    def update_proxys(self, proxys):
        if isinstance(proxys,dict) and proxys.__contains__('proxys'):
            super(Params,self).update(proxys)
        else:
            raise TypeError("Imput is not a dict, or don't have key 'proxys'")

    def update_types(self, types):
        if isinstance(types,dict) and types.__contains__('types'):
            super(Params,self).update(types)
        else:
            raise TypeError("Imput is not a dict, or don't have key 'types'")

    def update_point(self, points):
        if isinstance(points,dict) and points.__contains__('location') and points.__contains__('radius'):
            super(Params,self).update(points)
        else:
            raise TypeError("Imput is not a dict, or don't have key 'location' and 'radius'")

    def update_key(self, keys):
        if isinstance(keys,dict) and keys.__contains__('key'):
            super(Params,self).update(keys)
        else:
            raise TypeError("Imput is not a dict, or don't have key 'key'")


class Gpoi_Clawer(Clawer):
    def __init__(self, params):
        super(Gpoi_Clawer, self).__init__(params)
        self.url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
        self.key_type = u'谷歌'

    def scheduler(self):
        status_dict = {
            "OK": self.status_ok,
            "ZERO_RESULTS": self.status_pass,
            "OVER_QUERY_LIMIT": self.status_change_key,
            "REQUEST_DENIED": self.status_change_proxy,
            "INVALID_REQUEST": self.status_invalid_request,
            "UNKNOWN_ERROR": self.status_unknown_error
        }
        if self.respond != None :
            status = self.respond.get('status')
            if status:
                return status_dict[status]()
            else:
                self.status_change_user_agent()
        else:
            pass

    def status_ok(self):
        if 'next_page_token' not in self.respond:
            results = self.respond.get('results')
            if results:
                res_list = []
                for i in results:
                    res_list.append(self.parser(i))
                print('%s %s 采集成功' % (self.params['location'], self.params['types']))
                return res_list
            else:
                logger.info('结果为空 %s' % self.req_url)
        else:
            return '结果超出20个'


    def parser(self, json_dict):
        col_name = ['geometry_viewport_northeast_lat',
                    'geometry_viewport_northeast_lng',
                    'geometry_viewport_southwest_lat',
                    'geometry_viewport_southwest_lng',
                    'icon',
                    'photos_0_height',
                    'photos_0_width',
                    'photos_0_html_attributions_0',
                    'photos_0_photo_reference',
                    'photos_0_width scope',
                    'reference',
                    'scope'
                    ]
        res_dict = Res_Extractor().json_flatten(json_dict)
        for i in col_name:
            if i in res_dict:
                res_dict.pop(i)
        return res_dict



class Sample_Generator:
    def __init__(self, region_name, rect_list):
        self.rect_list = rect_list
        self.region_name = region_name
        self.radius_correct = self.filter_radius()


    def filter_radius(self):
        radius_correct = []
        rect_list = self.rect_list[:]
        while rect_list:
            rect = rect_list.pop()
            if rect.radius > 500:
                rect_list.extend(rect.divided_into_four())
            else:
                radius_correct.append(rect)
        print(u"%s 生成少于500m采样点 %s 个" % (self.region_name, len(radius_correct)))
        self.save_as_csv(radius_correct, '%s_point_by_radius.csv' % self.region_name)
        return radius_correct

    def save_as_csv(self,rect_list,file_path):
        a_list = [i.convert_to_df_dict() for i in rect_list]
        df = pd.DataFrame(a_list)
        df.to_csv(file_path, encoding= 'utf-8')

def main(region_name, rect_list):
    name = region_name
    res_floder = 'Gpoi_result/%s'% name
    type_changer = Type_Input('category.xlsx', 'ename', res_floder, method='add')
    type_list = [{'types': i} for i in type_changer.type_list]
    def by_type(poi_type):
        sample_list = Sample_Generator(name, rect_list).radius_correct
        def by_rect(rect):
            param = Params({'location':"39.915378,116.466935",
                                  'radius':"200",
                                  'types':"store",
                                  'key':"AIzaSyAqNRDfTrCWqlRiHSs2bpRaR0P85mbhAx0",
                                  })
            param.update_key(Key_Changer('谷歌').key_dict)
            param.update_types(poi_type)
            param.update_point(rect.convert_to_param_dict())
            gpoi_clawer = Gpoi_Clawer(param)
            res = gpoi_clawer.process()
            if isinstance(res, list):
                return res
            elif isinstance(res, str):
                return rect
            else:
                return

        result_list = []
        while sample_list:
            print('采集 %s，%s POI, 剩余 %s 个区域' % (name, poi_type, len(sample_list)))
            pool = ThreadPool()
            results = pool.map(by_rect, sample_list)
            pool.close()
            pool.join()
            sample_list = []
            for res in results:
                if isinstance(res, list):
                    result_list.extend(res)
                elif isinstance(res, Rectangle) and res.radius > 15:
                    sample_list.extend(res.divided_into_four())
                else:
                    pass

        res_saver = Res_saver(result_list, poi_type['types'], floder_path=res_floder, duplicates_key='place_id')
        res_saver.save_as_file()


    pool_v1 = ThreadPool()
    pool_v1.map(by_type, type_list)
    pool_v1.close()
    pool_v1.join()

def param_info(info_dict):
    info_table = prettytable.PrettyTable(['项目', '描述'])
    for key in list(info_dict.keys()):
        info_table.add_row([key, info_dict[key]])
    info_table.align = 'l'
    return str('\n' + str(info_table))




if __name__ == "__main__":
    rect_dict = {
        "白云区" : [Rectangle(113.1461246, 23.13955449, 113.5008903, 23.43149718)],
        "从化区" : [Rectangle(113.2738078, 23.37099304, 114.0565605, 23.93695479)],
        "番禺区" : [Rectangle(113.2429326, 22.87177748, 113.5533215, 23.08258251)],
        "海珠区" : [Rectangle(113.2333014, 23.04533721, 113.4122732, 23.11366537)],
        "花都区" : [Rectangle(112.9540515, 23.24907373, 113.4694197, 23.61688869)],
        "荔湾区" : [Rectangle(113.1706897, 23.0442161, 113.2693343, 23.15839047)],
        "黄埔区" : [Rectangle(113.389631, 23.03409065, 113.6017962, 23.42672447)],
        "南沙区" : [Rectangle(113.2911038, 22.56227328, 113.6843494, 22.90920969)],
        "天河区" : [Rectangle(113.2922662, 23.09766052, 113.4391771, 23.24457675)],
        "越秀区" : [Rectangle(113.2323543, 23.10463126, 113.3178628, 23.17175286)],
        "增城区" : [Rectangle(113.5406707, 23.08627615, 113.9949777, 23.62208945)]
    }
    start_time = datetime.datetime.now().strftime('%y-%m-%d %I:%M:%S %p')
    info_dict = {'名称': 'Google POI 抓取工具V1.0',
                 '邮箱': '575548935@qq.com',
                 '起始时间': start_time,
                 '终止时间': '20180401'
                 }
    logger.info(param_info(info_dict))
    for region_name, rect_list in rect_dict.items():
        main(region_name, rect_list)
    email_alarm = Email_alarm()
    end_time = datetime.datetime.now().strftime('%y-%m-%d %I:%M:%S %p')
    info_dict = {'名称': 'Google POI 抓取工具V1.0',
                 '邮箱': '575548935@qq.com',
                 '起始时间': start_time,
                 '终止时间': end_time
                 }
    email_alarm.send_mail(param_info(info_dict))





