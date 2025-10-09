package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);


    public Result queryById(Long id) {
        //缓存穿透queryWithPassThrough(id);

        //互斥锁解决缓存击穿queryWithMutex(id);

        //逻辑过期解决缓存击穿queryWithLogicalExpire(id);
        Shop shop = queryWithLogicalExpire(id);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        //返回
        return Result.ok(shop);
    }

    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        //先更新数据库
        updateById(shop);
        //再删redis缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    private boolean tryLock(String key){
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(b);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    public Shop queryWithMutex(Long id) {
        //查询redis缓存
        String shopJson= stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //存在直接返回
            Shop shop= JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if(shopJson!=null){
            //返回错误信息
            return null;
        }


        //开始缓存重建
        //获取互斥锁
        String lockKey=RedisConstants.CACHE_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否获取锁成功
            if(!isLock){
                //获取锁失败，休眠重试
               Thread.sleep(50);

                return queryWithMutex(id);
            }
            //查询redis缓存
            shopJson= stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            //判断是否存在
            if (StrUtil.isNotBlank(shopJson)) {
                //存在直接返回
                shop= JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }

            //不存在，查询数据库
            shop = getById(id);
            //数据库中不存在，返回错误
            if(shop==null){
                //为了防止缓存穿透，将空值写入redis
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //数据库中存在，写入redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+ id, JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            unLock(lockKey);
        }


        //返回
        return shop;
    }

    public Shop queryWithLogicalExpire(Long id) {
        //查询redis缓存
        String shopJson= stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //不存在直接返回空
            return null;
        }
        //命中，需要反序列化并判断过期时间
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return shop;
        }
        //过期，需要重建
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        //尝试获取锁
        boolean isLock = tryLock(lockKey);
        if(isLock){
            //再检查一次redis缓存是否过期，避免重复更新
            //查询redis缓存
            shopJson= stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            //命中，需要反序列化并判断过期时间
            redisData = JSONUtil.toBean(shopJson, RedisData.class);
            shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
            if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
                //未过期，直接返回店铺信息
                return shop;
            }
            //成功则开启线程重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //重建缓存
                    saveShop2Redis(id,30L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }


        //返回旧数据
        return shop;
    }


    public Shop queryWithPassThrough(Long id) {
        //查询redis缓存
        String shopJson= stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //存在直接返回
            Shop shop= JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if(shopJson!=null){
            //返回错误信息
            return null;
        }

        //不存在，查询数据库
        Shop shop = getById(id);
        //数据库中不存在，返回错误
        if(shop==null){
            //为了防止缓存穿透，将空值写入redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //数据库中存在，写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+ id, JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }

    public void saveShop2Redis(Long id, Long expireSeconds){
        //查询店铺信息
        Shop shop = getById(id);
        //封装数据
        RedisData redisData=new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }


}
