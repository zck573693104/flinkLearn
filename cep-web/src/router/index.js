import { createRouter, createWebHistory } from 'vue-router'
import Dashboard from '../views/Dashboard.vue'
import RuleEditor from '../views/RuleEditor.vue'
import AlertHistory from '../views/AlertHistory.vue'

const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: Dashboard,
    meta: { title: '监控面板' }
  },
  {
    path: '/rules',
    name: 'RuleList',
    component: () => import('../views/RuleList.vue'),
    meta: { title: '规则管理' }
  },
  {
    path: '/rule/new',
    name: 'RuleNew',
    component: RuleEditor,
    meta: { title: '新建规则' }
  },
  {
    path: '/rule/edit/:id',
    name: 'RuleEdit',
    component: RuleEditor,
    meta: { title: '编辑规则' }
  },
  {
    path: '/alert-history',
    name: 'AlertHistory',
    component: AlertHistory,
    meta: { title: '告警历史' }
  },
  {
    path: '/settings',
    name: 'Settings',
    component: () => import('../views/Settings.vue'),
    meta: { title: '系统设置' }
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

// 路由守卫：设置页面标题
router.beforeEach((to, from, next) => {
  document.title = to.meta.title ? `${to.meta.title} - Flink CEP` : 'Flink CEP 动态加载管理系统'
  next()
})

export default router
