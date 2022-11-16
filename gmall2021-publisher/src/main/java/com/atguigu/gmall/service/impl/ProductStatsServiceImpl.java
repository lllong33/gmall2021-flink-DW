package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.ProductStats;
import com.atguigu.gmall.mapper.ProductStatsMapper;
import com.atguigu.gmall.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by Enzo Cotter on 2022/11/16.
 */
@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
        return productStatsMapper.getProductStatsGroupBySpu(date, limit);
    }
    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsGroupByCategory3(date, limit);
    }
    @Override
    public List<ProductStats> getProductStatsByTrademark(int date,int limit) {
        return productStatsMapper.getProductStatsByTrademark(date, limit);
    }

}
