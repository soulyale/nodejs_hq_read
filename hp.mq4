//+------------------------------------------------------------------+
//|                                                           hq.mq4 |
//|                        Copyright 2017, MetaQuotes Software Corp. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2017, MetaQuotes Software Corp."
#property link      "https://www.mql5.com"
#property version   "1.00"
#property strict
const long INIT_GMT_OFFSET=11111;
struct Tbar{
   datetime time;
   double open;
   double high;
   double low;
};

struct Thq{
   double preday_close;
   long GMTOffset;
   Tbar m1;
   Tbar m5;
   Tbar m15;
   Tbar m30;
   Tbar h1;
   Tbar h4;
   Tbar d1;
   Tbar w1;
   Tbar mn1;
};
static Thq q;
int OnInit()
  {
    q.GMTOffset=INIT_GMT_OFFSET;
    EventSetTimer(1);  
    return(INIT_SUCCEEDED);
  }

void OnDeinit(const int reason)
  {
    EventKillTimer();
   
  }
void OnTick(){ 
   MqlTick  tick;
   if(SymbolInfoTick(_Symbol,tick)){
      if(q.GMTOffset==INIT_GMT_OFFSET){
         long cha=TimeGMT()-(tick.time_msc/1000);
         long abc=(long)tick.time;
        // if(cha%3600==0) q.GMTOffset=cha;      
        q.GMTOffset=cha;    
      }
      if(q.GMTOffset==INIT_GMT_OFFSET){
         static int count=0;
         count++;
         if(count>5) Alert(_Symbol+":时差错误");
         return;
      }
      string data=StringFormat("{\"datetime\":%d,\"ask\":%f,\"bid\":%f,\"volume\":%d",(long)tick.time+q.GMTOffset,tick.ask,tick.bid,tick.volume);
      if(q.preday_close!=iClose(_Symbol,PERIOD_D1,1)){
         q.preday_close =iClose(_Symbol,PERIOD_D1,1);
         data=data+StringFormat(",\"preday_close\":%f",q.preday_close);
      }
      data=data+TickUpdateBar("mn1",PERIOD_MN1,q.mn1);
      data=data+TickUpdateBar("w1",PERIOD_W1,q.w1);
      data=data+TickUpdateBar("d1",PERIOD_D1,q.d1);
      data=data+TickUpdateBar("h4",PERIOD_H4,q.h4);
      data=data+TickUpdateBar("h1",PERIOD_H1,q.h1);
      data=data+TickUpdateBar("m30",PERIOD_M30,q.m30);
      data=data+TickUpdateBar("m15",PERIOD_M15,q.m15);
      data=data+TickUpdateBar("m5",PERIOD_M5,q.m5);
      data=data+TickUpdateBar("m1",PERIOD_M1,q.m1);
      if(TimeGMT()%100==0){
         data=data+StringFormat(",\"gmt_time\":%f",TimeGMT());
       }
       data=data+"}";
       string url=PackUrl("http://127.0.0.1/current/quotes",_Symbol,0);
       string str=Post(url,data);
       RandMod();
   }
 }

void OnTimer()
  {
         if(q.GMTOffset==INIT_GMT_OFFSET) return;
         RefreshRates();
         if(HistoryUpdateBar(PERIOD_MN1,q.mn1)) return;
         if(HistoryUpdateBar(PERIOD_W1,q.w1)) return;
         if(HistoryUpdateBar(PERIOD_D1,q.d1)) return;
         if(HistoryUpdateBar(PERIOD_H4,q.h4)) return;
         if(HistoryUpdateBar(PERIOD_H1,q.h1)) return;
         if(HistoryUpdateBar(PERIOD_M30,q.m30)) return;
         if(HistoryUpdateBar(PERIOD_M15,q.m15)) return;
         if(HistoryUpdateBar(PERIOD_M5,q.m5)) return;
         if(HistoryUpdateBar(PERIOD_M1,q.m1)) return;
      
  }
  
void RandMod(){
       switch(((int)TimeGMT()%1000)){
          case 999:
            q.d1.open=0;
            q.d1.time=0;
            break;
          case 899:
            q.w1.open=0;
            q.w1.time=0;
            break;
          case 799:
            q.mn1.open=0;
            q.mn1.time=0; 
          case 699:
            q.preday_close=0;  
          default:
            break;
      } 
}
string TickUpdateBar(string timestr,ENUM_TIMEFRAMES timestamp,Tbar& obj){
   if(obj.high!=iHigh(_Symbol,timestamp,0) || obj.low !=iLow(_Symbol,timestamp,0)){
            obj.high =iHigh(_Symbol,timestamp,0);
            obj.low  =iLow(_Symbol,timestamp,0);
            return StringFormat(",\"%s\":{\"datetime\":%d,\"open\":%f,\"low\":%f,\"high\":%f,\"volume\":%d}",timestr,(long)iTime(_Symbol,timestamp,0)+q.GMTOffset,iOpen(_Symbol,timestamp,0),iLow(_Symbol,timestamp,0),iHigh(_Symbol,timestamp,0),iVolume(_Symbol,timestamp,0));
      }
   return "";
}
bool HistoryUpdateBar(ENUM_TIMEFRAMES timeframe,Tbar& obj){
    if(obj.time !=iTime(_Symbol,timeframe,0) || obj.open!=iOpen(_Symbol,timeframe,0)){
         if(UpdateHistory(timeframe)){
            obj.time=iTime(_Symbol,timeframe,0);
            obj.open=iOpen(_Symbol,timeframe,0);
            return true;
         }
      }
      return false;
}
bool UpdateHistory(ENUM_TIMEFRAMES timeframe){
   static int busy_timeout=0;
   if(busy_timeout>0) {
      busy_timeout--;
      return false;
   }
   string url=PackUrl("http://127.0.0.1/history/querymax",_Symbol,timeframe);
   string res= Get(url);//获取最大值
   int len=StringLen(res);
   if(len>0){////代表Get执行成功
      long start_time=StringToInteger(res);
      if(start_time>0){//表示查询成功
         MqlRates rates[];
         int num=CopyRates(_Symbol,timeframe,(datetime)(start_time-q.GMTOffset),TimeCurrent(),rates);
            if(num>1){//最少需要两条才能更新到服务器
               string data="[";
               for(int i=0;i<num;i++){
                  string tmp=StringFormat("{\"datetime\":\"%d\",\"open\":\"%f\",\"high\":\"%f\","+
                  "\"low\":\"%f\",\"close\":\"%f\",\"volume\":\"%d\"},",
                  rates[i].time+q.GMTOffset,rates[i].open,rates[i].high,rates[i].low,rates[i].close,rates[i].real_volume);
                  data+=tmp;
               }
               data=StringSubstr(data,0,StringLen(data)-1);
               data+="]";
               url="http://127.0.0.1/history/insert";
               url=PackUrl(url,_Symbol,timeframe);
               res=Post(url,data);
               if(StringToInteger(res)==num){
                  return true;
            }  
         }else{
               return true;
            }
      }else{
         busy_timeout+=10;
      }
   }
   return false;
}

string Post(string url,string data){
   char post[],ret[];
   ArrayResize(post,StringToCharArray(data,post,0,WHOLE_ARRAY,CP_UTF8)-1);
   if(200==WebRequest("POST",url,"Content-Type:application/json",2000,post,ret,data)){
      return CharArrayToString(ret,0,WHOLE_ARRAY,CP_ACP);
   }
   return "";
}

string Get(string url){
   char post[],ret[];
   string str;
    if(WebRequest("GET",url,"Content-Type:application/json",0,post,ret,str)==200){
      return CharArrayToString(ret,0,WHOLE_ARRAY,CP_ACP);
    }
    return "";
}
  string PackUrl(string url,string symbol,ENUM_TIMEFRAMES timeframe){

      if(StringToLower(symbol)){
         url=url+"/"+symbol+"/";
      }else{
         url=url+"/";
      }
      
      switch(timeframe){
          case PERIOD_M1:
            url+="m1";
            break;
          case PERIOD_M5:
            url+="m5";
            break;
          case PERIOD_M15:
            url+="m15";
            break;
          case PERIOD_M30:
            url+="m30";
            break;
          case PERIOD_H1:
            url+="h1";
            break;
          case PERIOD_H4:
            url+="h4";
            break;
          case PERIOD_D1:
            url+="d1";
            break;
          case PERIOD_W1:
            url+="w1";
            break;
          case PERIOD_MN1:
            url+="mn1";
            break;
          default:
            url=StringSubstr(url,0,StringLen(url)-1);
            break;
      } 
      return url;
  }