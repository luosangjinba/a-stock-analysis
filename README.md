# A股股票数据分析工具

获取A股股票行情和行业数据，支持多种数据源，定时更新并推送到 GitHub。

## 功能特性

- **多数据源支持**：Tushare、AKShare、Ashare（腾讯/新浪接口）
- **数据获取**：股票日线/分钟线行情、行业分类、资金流向
- **定时任务**：每日自动获取最新数据并提交到 GitHub
- **通知推送**：支持 Telegram 消息通知

## 文件说明

| 文件 | 说明 |
|------|------|
| `Ashare.py` | 股票行情数据获取库（腾讯/新浪接口） |
| `get_stock_data.py` | 股票数据获取主脚本 |
| `get_stock_data_tushare.py` | Tushare 数据源 |
| `get_stock_data_akshare.py` | AKShare 数据源 |
| `get_stock_data_ashare.py` | Ashare 数据源 |
| `fetch_industry_data.py` | 获取行业分类数据 |
| `add_industry.py` | 为股票添加行业信息 |
| `daily_task.py` | 每日定时任务脚本 |
| `daily_task_github.py` | GitHub 版每日任务 |
| `create_task.ps1` | Windows 定时任务创建脚本 |

## 数据文件

| 文件 | 说明 |
|------|------|
| `a_stock_top400.csv` | A股成交额前400股票 |
| `a_stock_top400_with_industry.csv` | 带行业分类的前400股票 |
| `industry_top12.csv` | 成交额前12行业 |
| `data/` | 每日行情数据目录 |

## 使用方法

### 1. 安装依赖

```bash
pip install tushare akshare pandas requests
```

### 2. 配置 Token

在脚本中设置你的 Tushare Token：

```python
TUSHARE_TOKEN = '你的token'
```

### 3. 获取股票数据

```python
from get_stock_data_tushare import get_stock_data
df = get_stock_data('2024-01-01', '2024-01-10')
```

### 4. 设置定时任务（Windows）

```powershell
powershell -ExecutionPolicy Bypass -File create_task.ps1
```

## 数据源

- **Tushare** - 收费 API，数据最全
- **AKShare** - 开源财经数据接口
- **Ashare** - 腾讯/新浪免费接口

## 注意事项

1. Tushare 需要注册获取 Token
2. 免费数据源有频率限制
3. 定时任务需要配置 Git 自动提交

## 许可证

MIT License
