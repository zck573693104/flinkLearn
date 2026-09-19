<template>
  <div class="alert-history">
    <el-card shadow="hover">
      <template #header>
        <div class="card-header">
          <span>告警历史记录</span>
          <div>
            <el-button type="primary" @click="refreshAlerts">刷新</el-button>
            <el-button @click="exportAlerts">导出 Excel</el-button>
          </div>
        </div>
      </template>
      
      <!-- 筛选条件 -->
      <el-form :inline="true" :model="filters" class="filter-form">
        <el-form-item label="规则名称">
          <el-input v-model="filters.ruleName" placeholder="请输入规则名称" clearable />
        </el-form-item>
        
        <el-form-item label="状态">
          <el-select v-model="filters.status" placeholder="请选择状态" clearable>
            <el-option label="已触发" value="triggered" />
            <el-option label="未触发" value="not_triggered" />
            <el-option label="已处理" value="handled" />
          </el-select>
        </el-form-item>
        
        <el-form-item label="时间范围">
          <el-date-picker
            v-model="dateRange"
            type="daterange"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            value-format="YYYY-MM-DD"
          />
        </el-form-item>
        
        <el-form-item>
          <el-button type="primary" @click="searchAlerts">查询</el-button>
          <el-button @click="resetFilters">重置</el-button>
        </el-form-item>
      </el-form>
      
      <!-- 告警列表 -->
      <el-table 
        :data="alerts" 
        style="width: 100%" 
        stripe
        v-loading="loading"
        @sort-change="handleSortChange"
      >
        <el-table-column type="selection" width="55" />
        
        <el-table-column prop="ruleName" label="规则名称" width="180" sortable />
        
        <el-table-column prop="eventType" label="事件类型" width="150" />
        
        <el-table-column prop="eventData" label="事件数据" min-width="250" show-overflow-tooltip />
        
        <el-table-column label="发生时间" width="180" sortable="custom">
          <template #default="scope">
            {{ formatTime(scope.row.timestamp) }}
          </template>
        </el-table-column>
        
        <el-table-column label="状态" width="120">
          <template #default="scope">
            <el-tag :type="getStatusType(scope.row.status)">
              {{ getStatusText(scope.row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        
        <el-table-column label="操作" width="150" fixed="right">
          <template #default="scope">
            <el-button size="small" @click="viewDetail(scope.row)">详情</el-button>
            <el-button size="small" type="primary" @click="handleAlert(scope.row)">处理</el-button>
          </template>
        </el-table-column>
      </el-table>
      
      <!-- 分页 -->
      <div class="pagination">
        <el-pagination
          v-model:current-page="pagination.page"
          v-model:page-size="pagination.size"
          :total="pagination.total"
          :page-sizes="[10, 20, 50, 100]"
          layout="total, sizes, prev, pager, next, jumper"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
        />
      </div>
    </el-card>
    
    <!-- 详情对话框 -->
    <el-dialog v-model="dialogVisible" title="告警详情" width="600px">
      <el-descriptions :column="1" border v-if="currentAlert">
        <el-descriptions-item label="告警 ID">{{ currentAlert.id }}</el-descriptions-item>
        <el-descriptions-item label="规则 ID">{{ currentAlert.ruleId }}</el-descriptions-item>
        <el-descriptions-item label="规则名称">{{ currentAlert.ruleName }}</el-descriptions-item>
        <el-descriptions-item label="事件类型">{{ currentAlert.eventType }}</el-descriptions-item>
        <el-descriptions-item label="事件数据">{{ currentAlert.eventData }}</el-descriptions-item>
        <el-descriptions-item label="发生时间">{{ formatTime(currentAlert.timestamp) }}</el-descriptions-item>
        <el-descriptions-item label="状态">
          <el-tag :type="getStatusType(currentAlert.status)">
            {{ getStatusText(currentAlert.status) }}
          </el-tag>
        </el-descriptions-item>
      </el-descriptions>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { ElMessage } from 'element-plus'

const loading = ref(false)
const alerts = ref([])
const filters = reactive({
  ruleName: '',
  status: ''
})
const dateRange = ref([])
const pagination = reactive({
  page: 1,
  size: 20,
  total: 0
})
const dialogVisible = ref(false)
const currentAlert = ref(null)

const refreshAlerts = () => {
  loadAlerts()
}

const searchAlerts = () => {
  pagination.page = 1
  loadAlerts()
}

const resetFilters = () => {
  filters.ruleName = ''
  filters.status = ''
  dateRange.value = []
  searchAlerts()
}

const loadAlerts = async () => {
  loading.value = true
  try {
    // TODO: 从 API 获取真实数据
    await new Promise(resolve => setTimeout(resolve, 500))
    
    alerts.value = [
      { id: 'alert_1', ruleId: 'rule_1', ruleName: '价格异常波动', eventType: 'PRICE_SPIKE', eventData: '{"price": 150}', timestamp: Date.now() - 3600000, status: 'triggered' },
      { id: 'alert_2', ruleId: 'rule_2', ruleName: '用户登录失败', eventType: 'LOGIN_FAILED', eventData: '{"user": "admin"}', timestamp: Date.now() - 7200000, status: 'handled' },
      { id: 'alert_3', ruleId: 'rule_3', ruleName: '库存不足预警', eventType: 'LOW_STOCK', eventData: '{"product": "iPhone"}', timestamp: Date.now() - 10800000, status: 'triggered' }
    ]
    pagination.total = 3
  } catch (error) {
    ElMessage.error('加载告警失败')
  } finally {
    loading.value = false
  }
}

const viewDetail = (row) => {
  currentAlert.value = row
  dialogVisible.value = true
}

const handleAlert = (row) => {
  ElMessage.success(`已处理告警：${row.ruleName}`)
  loadAlerts()
}

const formatTime = (timestamp) => {
  return new Date(timestamp).toLocaleString('zh-CN')
}

const getStatusType = (status) => {
  const types = { triggered: 'danger', not_triggered: 'success', handled: 'warning' }
  return types[status] || 'info'
}

const getStatusText = (status) => {
  const texts = { triggered: '已触发', not_triggered: '未触发', handled: '已处理' }
  return texts[status] || '未知'
}

const handleSizeChange = (val) => {
  pagination.size = val
  loadAlerts()
}

const handleCurrentChange = (val) => {
  pagination.page = val
  loadAlerts()
}

const handleSortChange = ({ prop, order }) => {
  console.log(prop, order)
}

const exportAlerts = () => {
  ElMessage.info('导出功能开发中...')
}

onMounted(() => {
  loadAlerts()
})
</script>

<style scoped>
.alert-history {
  padding: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.filter-form {
  margin-bottom: 20px;
}

.pagination {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
}
</style>
