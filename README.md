## jm-pgm-price-crawler

Johnson Matthey PGM price crawler（Playwright + pandas），用于抓取铂/钯/铑价格并保存到 Excel，方便本地分析和画走势图。

### 需要的文件（运行必备）

| 文件 | 说明 |
|------|------|
| **pgm_prices_crawler.py** | 主程序：抓取 Johnson Matthey 铂/钯/铑价格，写入 Excel，生成走势图 |
| **requirements_pgm_crawler.txt** | Python 依赖列表，安装：`pip install -r requirements_pgm_crawler.txt`，再执行 `python -m playwright install chromium` |

### 运行后生成的文件（可保留或删除）

| 文件 | 说明 |
|------|------|
| **all_metals_data.xlsx** | 抓取结果：三个 Sheet（Platinum / Palladium / Rhodium）+ 每页走势图。可保留作数据备份 |
| **PGM_debug_currentMetalPrices.json** | 调试用：当前价接口原始 JSON（仅当脚本内 `DEBUG_SAVE_RAW_TABLE = True` 时生成） |
| **PGM_debug_metalTablePrices.json** | 调试用：铂金分时表原始 JSON |
| **PGM_debug_parsed_meta.json** | 调试用：解析出的日期与 Pt 分时价格摘要 |

调试 JSON 可随时删除，不影响程序运行；若不需要调试，可在脚本里把 `DEBUG_SAVE_RAW_TABLE` 改为 `False`，之后不会再生成上述三个文件。

---

### 快速使用

```bash
pip install -r requirements_pgm_crawler.txt
python -m playwright install chromium
python pgm_prices_crawler.py          # 交互模式
python pgm_prices_crawler.py --once   # 单次抓取后退出
```

交互指令：`status` / `view` / `force` / `exit` / `help`。

---

### 项目说明（中文）

- **定位**：个人学习 / 数据分析用的 Johnson Matthey PGM（金属铂金、钯金、铑）价格抓取脚本，不提供任何投资建议。
- **功能**：
  - 使用 Playwright 访问 JM 官网，解析隐藏 JSON 数据；
  - 把 Pt/Pd/Rh 的价格写入同一个 `all_metals_data.xlsx`，每个金属一个 Sheet；
  - 每个 Sheet 自动生成折线走势图（按日期 × 四个时段列）；
  - 交互模式下可查看状态、强制抓取、打开 Excel 文件等。
- **注意**：使用本项目时，请自行阅读并遵守 Johnson Matthey 官网的 `Terms of Use` / `Acceptable Use Policy`，仅用于合规场景。

### Project description (English)

This repository contains a small Python script to scrape Johnson Matthey PGM (platinum group metals) prices (Platinum / Palladium / Rhodium) using Playwright, and save them into an Excel workbook (`all_metals_data.xlsx`) with one sheet per metal and a simple line chart on each sheet.  
It is intended for personal research and data analysis only. Please make sure you comply with Johnson Matthey's website Terms of Use and Acceptable Use Policy when using this tool.

---

### 如何上传到 GitHub（命令行方式）

1. 在 GitHub 网站新建一个空仓库，建议仓库名：`jm-pgm-price-crawler`（描述可以写：*Johnson Matthey PGM price crawler with Playwright and Excel output*）。
2. 在本机打开 PowerShell，进入项目目录：

```powershell
cd "c:\Users\charl\Desktop\metal"
```

3. 初始化 Git 仓库并提交当前代码：

```powershell
git init
git add .
git commit -m "Initial commit: jm-pgm-price-crawler"
git branch -M main
```

4. 关联 GitHub 远程仓库（把下面的 `YOUR_NAME` 和 `jm-pgm-price-crawler` 换成你在 GitHub 上实际创建的仓库地址）：

```powershell
git remote add origin https://github.com/YOUR_NAME/jm-pgm-price-crawler.git
```

5. 推送到 GitHub：

```powershell
git push -u origin main
```

推送成功后，在浏览器打开该 GitHub 仓库页面，就能看到 `pgm_prices_crawler.py`、`requirements_pgm_crawler.txt`、`README.md` 等文件。

