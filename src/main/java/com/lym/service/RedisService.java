package com.lym.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lym.util.RedisTemplate;
import org.apache.commons.lang.StringUtils;
import org.apache.http.util.Asserts;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * @ClassName RedisService
 * @Description 封装了常见REDIS缓存操作的service，需要构建RedisTemplate的bean名称为redis
 * @Author LYM
 * @Date 2018/7/20 16:58
 * @Version 1.0
 */
@Service
public class RedisService {

    @Autowired(required = false)
    protected RedisTemplate redis;

    protected ObjectMapper mapper = new ObjectMapper();

    @Autowired
    protected AsyncLoadCacheService asyncLoadCacheService;

    public RedisTemplate getRedis() {
        return redis;
    }

    public void setRedis(RedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * 校验参数不能为空
     */
    private void check() {
        Asserts.notNull(redis,"RedisTemplate");
        Asserts.notNull(mapper,"ObjectMapper");
    }

    /**
     * 创建流式API
     * 用例：
     * session.hget("key","field")
     *      .or(()->new ArrayList<T>())
     *      .expire(3600)
     *      .toArray();
     * 注意调用顺序不能混乱
     * @param clazz
     * @param <T>
     * @return
     */
    public <T> RedisSession<T> createSession(Class<T> clazz) {
        RedisSession<T> session = createSession(clazz,false);
        return session;
    }

    public <T> RedisSession<T> createSession(Class<T> clazz,boolean throughProtect) {
        check();
        RedisSession<T> session = new RedisSession<>(redis,mapper,clazz);
        session.throughProtect = throughProtect;
        return session;
    }

    /**
     * json转换成数组的映射
     * @param clazz
     * @return
     */
    public JavaType arrayType(Class clazz) {
        check();
        return mapper.getTypeFactory().constructCollectionType(ArrayList.class,clazz);
    }

    /**
     * json转换成单个元素的映射
     * @param clazz
     * @return
     */
    public JavaType classType(Class clazz) {
        check();
        return mapper.getTypeFactory().constructType(clazz);
    }

    /**
     * 获取hash中的元素并转换，若不存在则从supplier中获取
     * @param cache
     * @param key
     * @param supplier
     * @param jsonBind
     * @param <T>
     * @return
     */
    public <T> T hgetOr(String cache, String key, Supplier<T> supplier,JavaType jsonBind) {
        check();
        String json = redis.hget(cache,key);
        T result = null;
        if(StringUtils.isNotEmpty(json)) {
            try {
                result = mapper.readValue(json,jsonBind);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(result == null) {
            result = supplier.get();
            hset(cache,key,result);
        }
        return result;
    }

    /**
     * 获取hash中的数组并转换，若不存在则从supplier中获取
     */
    public <T> List<T> hgetListOr(String cache, String key, Supplier<List<T>> supplier, JavaType jsonBind) {
        check();
        List<T> result = hgetList(cache,key,jsonBind);
        if(result == null) {
            result = supplier.get();
            hset(cache,key,result);
        }
        return result;
    }

    /**
     * 获取hash中的数组并转换，若不存在则从supplier中获取
     */
    public <T> List<T> hgetListOr(String cache, String key, Supplier<List<T>> supplier, JavaType jsonBind,int expire) {
        check();
        List<T> result = hgetList(cache,key,jsonBind);
        if(result == null) {
            result = supplier.get();
            hset(cache,key,result);
            redis.expire(cache,expire);
        }
        return result;
    }

    /**
     * 将元素添加到json形式的数组中
     * @param cache
     * @param key
     * @param item
     * @param <T>
     * @return
     */
    public <T> boolean hAppendList(String cache,String key,T item) {
        check();
        List<T> result = hgetList(cache,key,arrayType(item.getClass()));
        if(result == null) {
            return false;
        }
        result.add(item);
        hset(cache,key,result);
        return true;
    }

    /**
     * 将元素替换到json形式的数组中，若不存在此元素则添加一个
     * @param cache
     * @param key
     * @param newItem
     * @param <T>
     * @return
     */
    public <T> boolean hReplaceList(String cache, String key, T newItem) {
        check();
        List<T> result = hgetList(cache,key,arrayType(newItem.getClass()));
        if(result == null) {
            result = new ArrayList<>();
        }
        boolean hasUpdate = false;
        for (T t : result) {
            if(newItem.equals(t)) {
                BeanUtils.copyProperties(newItem,t);
                hasUpdate = true;
                break;
            }
        }
        if(!hasUpdate) {
            result.add(newItem);
        }
        hset(cache,key,result);
        return true;
    }


    /**
     * 获取json数组
     * @param cache
     * @param key
     * @param jsonBind
     * @param <T>
     * @return
     */
    public <T> List<T> hgetList(String cache,String key,JavaType jsonBind) {
        check();
        String json = redis.hget(cache,key);
        List<T> result = null;
        if(StringUtils.isNotEmpty(json)) {
            try {
                result = mapper.readValue(json,jsonBind);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 将数据插入到某个Hash的key中
     * @param cache
     * @param key
     * @param obj
     */
    public void hset(String cache,String key,Object obj) {
        check();
        try {
            if(obj == null) {
                return;
            }
            redis.hset(cache,key,mapper.writeValueAsString(obj));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public <T> List<T> get(String key,JavaType jsonBind) {
        check();
        String json = redis.get(key);
        List<T> result = null;
        if(StringUtils.isNotEmpty(json)) {
            try {
                result = mapper.readValue(json,jsonBind);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public void set(String cache,Object obj) {
        check();
        try {
            if(obj == null) {
                return;
            }
            redis.set(cache,mapper.writeValueAsString(obj));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public <T> boolean hDelList(String cache, String key, T vo) {
        check();
        List<T> result = hgetList(cache,key,arrayType(vo.getClass()));
        if(result == null) {
            return false;
        }
        result.remove(vo);
        hset(cache,key,result);
        return true;
    }

    public <T> List<T> getListOr(String cache, Supplier<List<T>> supplier, JavaType jsonBind) {
        check();
        List<T> result = get(cache,jsonBind);
        if(result == null) {
            result = supplier.get();
            set(cache,result);
        }
        return result;
    }


    public <T> List<T> getListOr(String cache, Supplier<List<T>> supplier, JavaType jsonBind,int time) {
        check();
        List<T> result = get(cache,jsonBind);
        if(result==null){
            result = supplier.get();
            set(cache,result);
            redis.expire(cache,time);
        }
        return result;
    }

    /**
     * 支持流式API的redis会话
     * @param <T>
     */
    public class RedisSession<T> {
        private class Type {
            public static final int VALUE = 0;
            public static final int HASH = 1;
        }

        private static final String NODATA = "no data";

        private RedisTemplate redisTemplate;

        private ObjectMapper mapper;

        private Class<T> clazz;

        private String key,field,value;

        private int valueType;

        private Supplier supplier;

        private boolean orFlag = false;

        private boolean noDataFlag = false;

        private boolean throughProtect = false;


        public RedisSession(RedisTemplate redisTemplate,ObjectMapper mapper,Class<T> clazz) {
            this.redisTemplate = redisTemplate;
            this.mapper = mapper;
            this.clazz = clazz;
        }

        public RedisSession<T> setRedisTemplate(RedisTemplate redisTemplate) {
            this.redisTemplate = redisTemplate;
            return this;
        }

        public RedisSession<T> clearSession() {
            key = null;
            field = null;
            value = null;
            supplier = null;
            return this;
        }

        /**
         * 获取哈希中的值
         * @param key
         * @param field
         * @return
         */
        public RedisSession<T> hget(String key,String field) {
            value = redisTemplate.hget(key,field);
            this.key = key;
            this.field = field;
            this.valueType = Type.HASH;
            return this;
        }

        /**
         * 直接获取
         * @param key
         * @return
         */
        public RedisSession<T> get(String key) {
            value = redisTemplate.get(key);
            this.key = key;
            this.valueType = Type.VALUE;
            return this;
        }

        public RedisSession<T> lazyOr(Supplier supplier) {
            if(StringUtils.isEmpty(value)) {
                switch (valueType) {
                    case Type.VALUE:
                        asyncLoadCacheService.loadData(key,supplier);
                        break;
                    case Type.HASH:
                        asyncLoadCacheService.loadHashData(key,field,supplier);
                        break;
                    default:
                        break;
                }
            }
            return this;
        }

        /**
         * 当缓存中数据不存在时，从提供的supplier中获取
         * @param supplier
         * @return
         */
        public RedisSession<T> or (Supplier supplier) {
            if(StringUtils.isEmpty(value)){
                Object orv = supplier.get();
                try {
                    if( orv == null || (orv instanceof List && ((List) orv).size()==0)) {
                        if(noDataFlag) {
                            value = NODATA;
                        }
                    } else if( ! (orv instanceof String) ) {
                        value = mapper.writeValueAsString(orv);
                    } else {
                        value = (String) orv;
                    }
                    if(value == null) {
                        return this;
                    }
                    orFlag = true;
                    if(valueType == Type.VALUE) {
                        redisTemplate.set(key,value);
                    }else if(valueType == Type.HASH) {
                        redisTemplate.hset(key,field,value);
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
            return this;
        }

        public RedisSession<T> enableNoData(){
            noDataFlag = true;
            return this;
        }

        /**
         * 设置超时，默认仅当数据更新时设置
         * @param sec
         * @return
         */
        public RedisSession<T> expire(int sec) {
            expire(sec,false);
            return this;
        }

        /**
         * 设置超时，must=true，则立刻设置，must = false，则仅当数据更新时设置（即执行了or操作）
         * @param sec
         * @param must
         * @return
         */
        public RedisSession<T> expire(int sec, boolean must) {
            if(must || orFlag) {
                redisTemplate.expire(key,sec);
            }
            return this;
        }

        /**
         * 转成集合输出
         * @return
         */
        public List<T> toArray() {
            if(value == null || NODATA.equals(value) || StringUtils.isEmpty(value)) {
                return new ArrayList<>();
            }
            JavaType type = arrayType(clazz);
            try {
                return mapper.readValue(value,type);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return new ArrayList<>();
        }

        /**
         * 转成pojo输出
         * @return
         */
        public T toClass() {
            if(value == null || NODATA.equals(value) || StringUtils.isEmpty(value) ) {
                return null;
            }
            JavaType type = classType(clazz);
            try {
                return mapper.readValue(value,type);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        /**
         * 直接返回字符串形式的value
         * @return
         */
        public String toStr() {
            return value;
        }

        public void set(String key,Object obj,int expire) {
            set(key,obj);
            expire(expire,true);
        }

        public void set(String key,Object obj) {
            try {
                this.key = key;
                value = mapper.writeValueAsString(obj);
                redisTemplate.set(key,value);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        public void hset(String key,String field,Object obj) {
            try {
                this.key = key;
                this.field = field;
                value = mapper.writeValueAsString(obj);
                redisTemplate.hset(key,field,value);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        /**
         * 插入到数组中，注意实现equal方法，以实现替换原有pojo
         * @param key
         * @param obj
         */
        public void insertToArray(String key, T obj) {
            List<T> list = get(key).toArray();
            int ind = list.indexOf(obj);
            if(ind > 0) {
                list.remove(ind);
            }
            list.add(obj);
            set(key, list);
        }

        /**
         * 插入到数组中，注意实现equal方法，以实现替换原有pojo
         * @param key
         * @param obj
         */
        public void hInsertToArray(String key, String field, T obj) {
            List<T> list = hget(key,field).toArray();
            int ind = list.indexOf(obj);
            if(ind > 0) {
                list.remove(ind);
            }
            list.add(obj);
            hset(key,field,list);
        }
    }

}
