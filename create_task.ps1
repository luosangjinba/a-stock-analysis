$action = New-ScheduledTaskAction -Execute 'py.exe' -Argument 'G:\MiniMAX-agent\project\a_stock_analysis\daily_task.py' -WorkingDirectory 'G:\MiniMAX-agent\project\a_stock_analysis'
$trigger = New-ScheduledTaskTrigger -Daily -At '16:30'
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskName 'AStockIndustryUpdate' -Action $action -Trigger $trigger -Settings $settings -Force
Write-Host "Task created successfully!"
