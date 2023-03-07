package cn.shh.test.es.service.impl;

import cn.shh.test.es.pojo.Hotel;
import cn.shh.test.es.mapper.HotelMapper;
import cn.shh.test.es.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author haoge
 * @since 2022-09-07
 */
@Service
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

}
