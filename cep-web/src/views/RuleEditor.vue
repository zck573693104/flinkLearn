<template>
  <div class="rule-editor">
    <el-card>
      <template #header>
        <div class="card-header">
          <span>CEP 规则配置</span>
          <el-button type="primary" @click="saveRule">保存规则</el-button>
        </div>
      </template>
      
      <el-form :model="ruleConfig" label-width="120px">
        <el-form-item label="规则 ID">
          <el-input v-model="ruleConfig.ruleId" placeholder="规则唯一标识" />
        </el-form-item>
        
        <el-form-item label="规则名称">
          <el-input v-model="ruleConfig.ruleName" placeholder="规则显示名称" />
        </el-form-item>
        
        <el-form-item label="启用状态">
          <el-switch v-model="ruleConfig.enabled" />
        </el-form-item>
        
        <el-form-item label="Pattern 定义">
          <el-input
            v-model="ruleConfig.patternJson"
            type="textarea"
            :rows="15"
            placeholder='{"name": "price_spike", "times": null, ...}'
          />
          <div class="pattern-help">
            <p>Pattern 定义格式请参考：<a href="#" target="_blank">Flink CEP 文档</a></p>
          </div>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive } from 'vue'
import { createRule, updateRule } from '@/api/cep'

const ruleConfig = reactive({
  ruleId: '',
  ruleName: '',
  patternJson: JSON.stringify({
    name: 'example',
    times: null,
    timesMin: null,
    of: {
      first: {
        type: 'simple',
        condition: {
          field: 'value',
          operator: '>',
          value: 100
        }
      }
    },
    where: {
      type: 'strict',
      duration: 5,
      unit: 'minutes'
    }
  }, null, 2),
  enabled: true
})

const saveRule = async () => {
  try {
    if (!ruleConfig.ruleId) {
      ElMessage.error('请输入规则 ID')
      return
    }
    
    // TODO: 调用 API 保存规则
    const result = await createRule(ruleConfig)
    ElMessage.success('规则保存成功')
  } catch (error) {
    ElMessage.error('保存失败：' + error.message)
  }
}
</script>

<style scoped>
.rule-editor {
  padding: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.pattern-help {
  margin-top: 10px;
  font-size: 12px;
  color: #999;
}

.pattern-help a {
  color: #409eff;
}
</style>
