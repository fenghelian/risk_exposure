#coding=utf-8
import util
import calendar 
from matplotlib.testing.jpl_units import day
class Entity():
    def __init__(self, year, month, day, money, index):
        #0 是周一
        self._weekday = calendar.weekday(int(year), month, day)
        self._day = day
        self._month = month
        self._year = int(year)
        self._money = money
        #该月的第几周
        self._index = index
        monthRange = calendar.monthrange(int(year), month)
        self._rate = 1.0/monthRange[1]
        self._count = 1
        
    def to_string(self):
        return str(self._rate) + " : 周=" + str(self._weekday + 1) + ": day=" + str(self._day) + ": amount=" + str(self._money)
        

class EstimateAmount():
    def __init__(self, product_amount_history_file, n_size = 3):
        self._n_size = n_size
        self._product_table = {}
        self._product_amount_history_file = product_amount_history_file
        self._real_month = set()
    
    def run(self, product_amount_file):
        '''
            先初始化历史数据，根据规则算法推算目标账期的每天的金额
            formate:
                dt,type,orign_amount
        '''
        #构造历史账期数据
        self.read_money_history()
        #读取目标账期数据，需要估计出来
        for line in open(product_amount_file):
            ar = line.strip().split(',')
            if len(ar) != 3:continue
            if ar[0].strip() == 'dt':
                continue
            date = ar[0]
            type = ar[1]
            origin_amount = float(ar[2])
            print "----",line.strip(),"------------"
            self.estimate_money(date, self._n_size, origin_amount, type)
        pass
    
    
    def read_money_history(self):
        '''
            按照产品区分
        '''
        config = open(self._product_amount_history_file) 
        for line in config:
            ar = line.strip().split(',')
            if len(ar) == 0:continue
            if ar[0] == 'dt':#过滤title
                continue
            #历史数据中每天的金额
            value = float(ar[2])
            #产品类型
            type = ar[1]
            if not self._product_table.has_key(type):
                self._product_table[type] = {}
            yMd = ar[0].split('-')
            year = yMd[0]
            month = int(yMd[1])
            self._real_month.add(yMd[1])
            day = int(yMd[2])
            if self._product_table[type].has_key(year):
                #如果该月已经初始化
                if self._product_table[type][year][month - 1]:
                    for ele in self._product_table[type][year][month - 1]:
                        if ele._day == day:
                            ele._money = value
                else:#如果该月还没有初始化
                    month_objarray = self.init_month_entity(year, month, day, value)
                    self._product_table[type][year][month - 1] = month_objarray
            else:
                #初始化该年所有月
                temp = [None] * 12
                #得到本月所有的周
                month_objarray = self.init_month_entity(year, month, day, value)
                temp[month -1] = month_objarray
                self._product_table[type][year] = temp
        #print(self._product_table)

    def init_month_entity(self, year, month, day, money):
        month_objarray= []
        '''
            每次推算当月所有的周和日关系
        '''
        first = calendar.monthcalendar(int(year), month)
        for index, a in enumerate(first):
            for b in a:
                if b != 0:
                    if b == day:
                        month_objarray.append(Entity(year, month, b, money, index))
                    else:
                        month_objarray.append(Entity(year, month, b, 0, index))
                    
        return month_objarray
        
        
    def estimate_money(self, special_date, n_size, month_money, type):
        '''
           按照具体产品输出 
           按照新规则重写需要
        '''
        yM = special_date.split('-')
        year = yM[0].strip()
        month = int(yM[1])
        #如果跨年
        if not self._product_table[type].has_key(year):
            #初始化该年所有月
            temp = [None] * 12
            #得到本月所有的周
            month_objarray = self.init_month_entity(year, month, 0, 0)
            temp[month -1] = month_objarray
            self._product_table[type][year] = temp
        
        current_month_data = self._product_table[type][year][month - 1]
        if current_month_data == None:
            print "如果该月没有预估过 需要初始化", month
            self._product_table[type][year][month - 1] = self.init_month_entity(year, month, 0, 0)
            current_month_data = self._product_table[type][year][month - 1]
        if n_size > len(self._real_month):
            n_size = len(self._real_month)
        #获取前几个月的范围    
        pre_months = util.get_month_range(int(year), month, 1, n_size)
        for pre_date in pre_months: 
            if not self._product_table[type].has_key(pre_date[0]):
                continue
            if len(self._product_table[type][pre_date[0]]) <= pre_date[1] - 1:
                continue
            history_month_data = self._product_table[type][pre_date[0]][pre_date[1] - 1]
            if history_month_data == None:
                self._product_table[type][pre_date[0]][pre_date[1] - 1] = self.init_month_entity(pre_date[0], pre_date[1], 0, 0)
                history_month_data = self._product_table[type][pre_date[0]][pre_date[1] - 1]
            #找出指定月份的所有值，当前要预估的月份的周信息是策略的重要依据
            '''
                按照周几来预估，比如本月的第一个周1需要使用前n_size个月的第一个周一的金额占比均值估计，第一个周二使用上n_size个第一个周二，
                第二个周一使用上n_size个月的第二个周一的金额占比均值，依次类推。。。。。，如果周的个数不匹配则需要分两种情况
                1：预估月的周多，此时使用历史月的最后一个周对应的周号
            '''
            for curr_data in current_month_data:
                #当前循环的历史月的总金额
                histor_all_money = 0.0
                for data in history_month_data:
                    histor_all_money += data._money
                #求历史月份在本月每天金额占比
                for data in history_month_data:
                    data._rate = data._money / (histor_all_money + 1)
                #预估主策略：相同顺序周并且周号相同，占比累计
                #获取当月日历信息，用以探测对应的周号是否存在相同周（跨月的情况需要处理）
                
                pre_month_calendar = calendar.monthcalendar(int(pre_date[0]), pre_date[1])
                #当前周号在上一个月中是否存在，如果不对齐 当前月的周数小于上一个月的周数，前进一周
                day = 0
                if len(pre_month_calendar) == curr_data._index + 1:
                    week_calendar = pre_month_calendar[curr_data._index]
                else:
                    week_calendar = pre_month_calendar[curr_data._index - 1]
                for index, week_code in enumerate(week_calendar):
                    if week_code != 0 and curr_data._weekday == index:
                        #print curr_data._day, curr_data._weekday, index, month - count, week_code
                        day = week_code
                        
                if day == 0:#如果对应周没有周一，则顺延一周
                    #print curr_data._index,month - count, day
                    week_calendar = pre_month_calendar[curr_data._index - 1]
                    for index, week_code in enumerate(week_calendar):
                        if week_code != 0 and curr_data._weekday == index:
                            day = week_code
                for data in history_month_data:
                    #定位day
                    if data._day == day:
                        curr_data._rate += data._rate
                        if data._rate > 0:
                            curr_data._count += 1
                        '''
                        如果 周号相同，break， 如果全部扫描后没有就用后延第一个相同周
                        '''
                        break;       
            #day_weight = math_util.scale_by_zscore(self._product_table[type][year][month - count - 1])
            #day_weights.append(day_weight * month_money)
        
        all_rate = 0
        for month_pred in current_month_data:
            all_rate += month_pred._rate / month_pred._count
            if month_pred._rate == 0:
                print month_pred.to_string()
        for month_pred in current_month_data:
            #print month_pred.to_string()
            if all_rate != 0:
                #month_pred._rate = (month_pred._rate / month_pred._count) / (all_rate) 
                month_pred._money = (month_pred._rate / month_pred._count) / (all_rate) * month_money
                month_pred._rate = (month_pred._rate / month_pred._count) / (all_rate) 
           
        
    def save_to_csv(self, outfile):
        '''
            保存到csv文件
        '''
        for key, val in self._product_amount_history_file.item():
            print key, val
        pass
    
    def get_estimated_money(self, special_date, type):
        yM = special_date.strip().split('-')
        year = yM[0]
        month = int(yM[1])
        return self._product_table[type][year][month - 1]
     
