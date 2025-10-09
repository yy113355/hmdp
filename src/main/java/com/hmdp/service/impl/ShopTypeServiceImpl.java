package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;


    public Result queryTypeList() {
        //查询redis缓存
        String shopTypeJson = stringRedisTemplate.opsForValue().get("cache:shoptype:list");
        //判断是否存在
        if (StrUtil.isNotBlank(shopTypeJson)) {
            //存在直接返回
            List<ShopType> typeList = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(typeList);
        }
        //不存在，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        //数据库中不存在，返回错误
        if(typeList==null){
            return Result.fail("店铺类型不存在");
        }
        //数据库中存在，写入redis
        stringRedisTemplate.opsForValue().set("cache:shoptype:list", JSONUtil.toJsonStr(typeList));
        //返回
        return Result.ok(typeList);
    }
}
