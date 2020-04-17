package org.fuwushe.sink.mapper;

import org.apache.ibatis.annotations.Param;

public interface TestMapper {

    void save(@Param("name") String name);
}
