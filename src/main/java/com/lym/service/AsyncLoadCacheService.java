package com.lym.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lym.util.RedisTemplate;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 异步懒加载缓存机制
 * 限制从数据库读取数据填充缓存的动作，避免当缓存失效等情况下压垮数据库
 * 1 - 当数据不存在时提出加载请求
 * 2 - 判定加载锁
 * 3 - 锁失效时，加载一次
 * 4 - 加载完成后不论成功失败均重新上锁
 *
 * 注意：
 * 1 - 当缓存不存在时，本次请求不会返回数据
 * 2 - 当缓存失效时，一定时间内不会及时填充缓存数据
 *
 * 隐含逻辑基础：
 * 1 - redis是不可靠的
 * 2 - 必要情况下是可以放弃数据，优先保障服务的。
 * 3 - 接口不等待数据库，仅获取缓存中的数据，意味着即使数据库挂掉也可以正常访问。
 */
@Component
public class AsyncLoadCacheService {

    private static final Map<String,Long> CACHE_DIS_MAPPER;

    static {
        CACHE_DIS_MAPPER = new ConcurrentHashMap<>();

    }

    @Autowired(required = false)
    private RedisTemplate redisTemplate;

    @Autowired(required = false)
    private ObjectMapper mapper;

    @Async
    public void loadData(String key,Supplier supplier){
        if(!authLoadLock(genKey(key))){
            return;
        }
        long start = System.currentTimeMillis();
        String value = "";
        Object obj = supplier.get();
        try {
            value = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        redisTemplate.set(key,value);
        long dif = System.currentTimeMillis()-start;
        setLoadLock(genKey(key),dif);
    }

    @Async
    public void loadHashData(String key, String field, Supplier supplier){
        if(!authLoadLock(genKey(key,field))){
            return;
        }
        long start = System.currentTimeMillis();
        String value = "";
        Object obj = supplier.get();
        try {
            value = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        redisTemplate.hset(key,field,value);
        long dif = System.currentTimeMillis()-start;
        setLoadLock(genKey(key,field),dif);
    }

    //校验是否允许加载缓存
    private synchronized boolean authLoadLock(String lockKey){
        if(CACHE_DIS_MAPPER.containsKey(lockKey)){
            Long time = CACHE_DIS_MAPPER.get(lockKey);
            if(time > System.currentTimeMillis()){
                return false;
            }
        }
        CACHE_DIS_MAPPER.put(lockKey,System.currentTimeMillis()+5*1000);//给你五秒钟来读取数据
        return true;
    }

    private String genKey(String... keys){
        return StringUtils.join(keys,"_");
    }

    private void setLoadLock(String loadLock,long costTime) {
        //读取数据库耗时较长
        if(costTime>2000){
            CACHE_DIS_MAPPER.put(loadLock,System.currentTimeMillis()+30*60*1000);//半小时内不允许加载-数据库很可能挂掉了
        }else{
            CACHE_DIS_MAPPER.put(loadLock,System.currentTimeMillis()+60000);//一分钟内不允许加载，那么快干啥
        }

    }

}
