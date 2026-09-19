<template>
  <div class="dashboard">
    <!-- 统计卡片 -->
    <el-row :gutter="20" style="margin-bottom: 20px;">
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-icon" style="background-color: #409EFF;">
            <el-icon><Monitor /></el-icon>
          </div>
          <div class="stat-info">
            <div class="stat-value">{{ metrics.activeRules }}</div>
            <div class="stat-label">活跃规则</div>
          </div>
        </el-card>
      </el-col>
      
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-icon" style="background-color: #67C23A;">
            <el-icon><CircleCheck /></el-icon>
          </div>
          <div class="stat-info">
            <div class="stat-value">{{ metrics.totalEvents }}</div>
            <div class="stat-label">处理事件</div>
          </div>
        </el-card>
      </el-col>
      
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-icon" style="background-color: #E6A23C;">
            <el-icon><Warning /></el-icon>
          </div>
          <div class="stat-info">
            <div class="stat-value">{{ metrics.alertCount }}</div>
            <div class="stat-label">告警次数</div>
          </div>
        </el-card>
      </el-col>
      
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-icon" style="background-color: #F56C6C;">
            <el-icon><RefreshRight /></el-icon>
          </div>
          <div class="stat-info">
            <div class="stat-value">{{ metrics.processingTime }}ms</div>
            <div class="stat-label">平均耗时</div>
          </div>
        </el-card>
      </el-col>
    </el-row>
    
    <!-- 图表区域 -->
    <el-row :gutter="20" style="margin-bottom: 20px;">
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header>
            <div class="card-header">
              <span>实时事件流量</span>
              <el-button size="small" @click="refreshMetrics">刷新</el-button>
            </div>
          </template>
          <div ref="trafficChartRef" style="height: 300px;"></div>
        </el-card>
      </el-col>
      
      <el-col :span="12">
        <el-card shadow="hover">
          <template #header>
            <div class="card-header">
              <span>规则分布</span>
            </div>
          </template>
          <div ref="distributionChartRef" style="height: 300px;"></div>
        </el-card>
      </el-col>
    </el-row>
    
    <!-- 最近告警 -->
    <el-card shadow="hover">
      <template #header>
        <div class="card-header">
          <span>最近告警记录</span>
          <el-button type="primary" size="small" @click="$router.push('/alert-history')">查看全部</el-button>
        </div>
      </template>
      
      <el-table :data="recentAlerts" style="width: 100%" stripe>
        <el-table-column prop="ruleName" label="规则名称" width="180" />
        <el-table-column prop="eventType" label="事件类型" width="120" />
        <el-table-column prop="timestamp" label="发生时间" width="180" />
        <el-table-column label="状态">
          <template #default="scope">
            <el-tag :type="scope.row.status === 'triggered' ? 'danger' : 'success'">
              {{ scope.row.status === 'triggered' ? '已触发' : '未触发' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import * as echarts from 'echarts'
import { Monitor, CircleCheck, Warning, RefreshRight } from '@element-plus/icons-vue'
import { getAllRules, getMetrics } from '@/api/cep'

const metrics = ref({
  activeRules: 0,
  totalEvents: 0,
  alertCount: 0,
  processingTime: 0
})

const trafficChartRef = ref(null)
const distributionChartRef = ref(null)

const recentAlerts = ref([
  { ruleName: '价格异常波动', eventType: 'PRICE_SPIKE', timestamp: '2024-01-20 10:30:00', status: 'triggered' },
  { ruleName: '用户登录失败', eventType: 'LOGIN_FAILED', timestamp: '2024-01-20 10:25:00', status: 'triggered' },
  { ruleName: '库存不足预警', eventType: 'LOW_STOCK', timestamp: '2024-01-20 10:20:00', status: 'triggered' }
])

const refreshMetrics = async () => {
  try {
    const rulesResponse = await getAllRules()
    metrics.value.activeRules = Object.keys(rulesResponse.data || {}).length
    
    // TODO: 从 API 获取真实数据
    metrics.value.totalEvents = 1250
    metrics.value.alertCount = 45
    metrics.value.processingTime = 120
    
    updateTrafficChart()
    updateDistributionChart()
  } catch (error) {
    console.error('获取指标失败:', error)
  }
}

const updateTrafficChart = () => {
  if (!trafficChartRef.value) return
  
  const chart = echarts.init(trafficChartRef.value)
  const option = {
    tooltip: {
      trigger: 'axis'
    },
    legend: {
      data: ['流入事件', '流出事件']
    },
    xAxis: {
      type: 'category',
      data: ['10:00', '10:05', '10:10', '10:15', '10:20', '10:25', '10:30']
    },
    yAxis: {
      type: 'value'
    },
    series: [
      {
        name: '流入事件',
        type: 'line',
        data: [120, 132, 101, 134, 90, 230, 210],
        smooth: true
      },
      {
        name: '流出事件',
        type: 'line',
        data: [220, 182, 191, 234, 290, 330, 310],
        smooth: true
      }
    ]
  }
  chart.setOption(option)
}

const updateDistributionChart = () => {
  if (!distributionChartRef.value) return
  
  const chart = echarts.init(distributionChartRef.value)
  const option = {
    tooltip: {
      trigger: 'item'
    },
    legend: {
      orient: 'vertical',
      left: 'left'
    },
    series: [
      {
        name: '规则类型',
        type: 'pie',
        radius: '50%',
        data: [
          { value: 1048, name: '价格监控' },
          { value: 735, name: '安全告警' },
          { value: 580, name: '库存管理' },
          { value: 484, name: '用户行为' },
          { value: 300, name: '其他' }
        ],
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        }
      }
    ]
  }
  chart.setOption(option)
}

onMounted(() => {
  refreshMetrics()
  
  // 定时刷新
  setInterval(refreshMetrics, 30000)
})
</script>

<style scoped>
.dashboard {
  padding: 20px;
}

.stat-card {
  display: flex;
  align-items: center;
  padding: 10px;
}

.stat-icon {
  width: 60px;
  height: 60px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 15px;
}

.stat-icon .el-icon {
  font-size: 28px;
  color: white;
}

.stat-info {
  flex: 1;
}

.stat-value {
  font-size: 28px;
  font-weight: bold;
  color: #333;
  margin-bottom: 5px;
}

.stat-label {
  font-size: 14px;
  color: #999;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
