#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M5 项目一键全链路启动脚本
实验14任务1：服务编排与进程管理

全链路调度拓展说明：
========================================
支持两种启动模式：
1. 完整全链路模式（默认）：python run_app.py
   自动依次串行执行 m1→m2→m3→m4
2. 快速调试看板模式：python run_app.py --dashboard-only
   仅启动m4看板服务，跳过前置模块

Windows系统端口残留问题修复：
========================================
⚠️ 重要说明：Windows环境不推荐使用 uvicorn --reload 模式
原因：--reload 模式会启动主进程+工作进程双进程架构，
      普通terminate()只能终止主进程，工作进程会变成孤儿进程，
      继续占用端口造成僵尸占用，需手动释放。

修复方案：
1. 移除--reload参数，使用单进程模式运行
2. 使用taskkill /F /T强制终止进程树
3. 端口检测时过滤PID=0的无效连接
4. 端口占用时自动查找并释放
"""

import subprocess
import os
import webbrowser
import signal
import time
import socket
import sys
import argparse


class Color:
    """控制台颜色输出类"""
    INFO = '\033[94m'
    SUCCESS = '\033[92m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    RESET = '\033[0m'


# 全局变量：存储所有子进程引用，用于统一管理
child_processes = []


def check_port(port):
    """检查端口是否可用（通过socket连接检测）"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    result = sock.connect_ex(('127.0.0.1', port))
    sock.close()
    return result != 0


def get_port_occupants(port):
    """
    获取占用端口的真实监听进程PID列表
    优化：过滤PID=0的无效连接，仅返回真实监听进程
    """
    occupants = []
    if os.name == 'nt':
        try:
            result = subprocess.run(
                ['netstat', '-ano'],
                capture_output=True,
                text=True,
                shell=True
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    # 只关注 LISTENING 状态的连接
                    if f':{port}' in line and 'LISTENING' in line:
                        parts = line.split()
                        if len(parts) >= 5:
                            pid = parts[-1]
                            if pid.isdigit():
                                pid_int = int(pid)
                                # 过滤PID=0的无效系统连接
                                if pid_int != 0:
                                    occupants.append(pid_int)
        except Exception as e:
            print(f"{Color.WARNING}[WARN] 查询端口占用失败: {e}{Color.RESET}")
    # 去重并返回
    return list(set(occupants))


def kill_process_tree(pid):
    """
    强制终止进程及其所有子进程（使用taskkill /F /T）
    /F: 强制终止
    /T: 终止进程树（包括所有子进程）
    """
    if os.name == 'nt':
        try:
            print(f"{Color.INFO}[INFO] 终止进程树，根PID: {pid}{Color.RESET}")
            result = subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(pid)],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"{Color.SUCCESS}[SUCCESS] 进程树 {pid} 已终止{Color.RESET}")
                return True
            else:
                print(f"{Color.WARNING}[WARN] taskkill返回错误: {result.stderr}{Color.RESET}")
                return False
        except Exception as e:
            print(f"{Color.WARNING}[WARN] 执行taskkill失败: {e}{Color.RESET}")
            return False
    return False


def release_port(port):
    """释放被占用的端口"""
    occupants = get_port_occupants(port)
    if occupants:
        print(f"{Color.WARNING}[WARN] 端口 {port} 被进程占用: {occupants}{Color.RESET}")
        for pid in occupants:
            print(f"{Color.INFO}[INFO] 终止进程 {pid}...{Color.RESET}")
            if kill_process_tree(pid):
                print(f"{Color.SUCCESS}[SUCCESS] 进程 {pid} 及其子进程已终止{Color.RESET}")
            else:
                print(f"{Color.ERROR}[ERROR] 无法终止进程 {pid}{Color.RESET}")
        time.sleep(1)
        return check_port(port)
    return True


def env_check(root_dir):
    """环境自检：校验业务目录和server.py文件"""
    print(f"{Color.INFO}[INFO] === 环境自检 ==={Color.RESET}")
    
    sub_dirs = ['m1_data_clean', 'm2_stream_sim', 'm3_llm_feature', 'm4_dashboard']
    missing = []
    for d in sub_dirs:
        path = os.path.join(root_dir, d)
        if not os.path.exists(path):
            missing.append(d)
    
    if missing:
        print(f"{Color.ERROR}[ERROR] 缺失业务目录: {', '.join(missing)}{Color.RESET}")
        return False
    print(f"{Color.SUCCESS}[SUCCESS] 所有业务目录校验通过{Color.RESET}")
    
    server_path = os.path.join(root_dir, 'm4_dashboard', 'server.py')
    if not os.path.exists(server_path):
        print(f"{Color.ERROR}[ERROR] 缺失服务文件: {server_path}{Color.RESET}")
        return False
    print(f"{Color.SUCCESS}[SUCCESS] server.py 文件校验通过{Color.RESET}")
    
    return True


def run_module(module_name, module_dir, script_name, root_dir):
    """
    执行单个模块的入口脚本
    :param module_name: 模块名称（用于日志输出）
    :param module_dir: 模块目录名
    :param script_name: 入口脚本名称
    :param root_dir: 项目根目录
    :return: True表示执行成功，False表示失败
    """
    script_path = os.path.join(root_dir, module_dir, script_name)
    
    if not os.path.exists(script_path):
        print(f"{Color.WARNING}[WARN] 模块 {module_name} 入口脚本不存在: {script_path}")
        print(f"{Color.WARNING}[WARN] 跳过该模块执行{Color.RESET}")
        return True
    
    print(f"\n{Color.INFO}[INFO] === [{module_name}] 开始执行 ===")
    print(f"{Color.INFO}[INFO] 入口脚本: {script_path}{Color.RESET}")
    
    # 获取当前Python解释器路径（确保使用虚拟环境中的Python）
    python_executable = sys.executable
    print(f"{Color.INFO}[INFO] Python解释器: {python_executable}{Color.RESET}")
    
    # 构建执行命令
    cmd = [python_executable, script_name]
    
    try:
        # 串行阻塞执行，等待模块运行完毕
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.join(root_dir, module_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        )
        
        # 记录子进程引用
        child_processes.append(proc)
        
        # 实时输出模块执行日志
        while True:
            line = proc.stdout.readline()
            if line == '' and proc.poll() is not None:
                break
            if line:
                print(f"{Color.INFO}[{module_name}] {line.rstrip()}{Color.RESET}")
        
        # 检查执行结果
        returncode = proc.wait()
        if returncode == 0:
            print(f"{Color.SUCCESS}[SUCCESS] [{module_name}] 执行完成{Color.RESET}")
            return True
        else:
            print(f"{Color.ERROR}[ERROR] [{module_name}] 执行失败，返回码: {returncode}{Color.RESET}")
            return False
            
    except Exception as e:
        print(f"{Color.ERROR}[ERROR] [{module_name}] 执行异常: {e}{Color.RESET}")
        return False


def check_module_output(module_name, root_dir):
    """
    检查模块是否已经执行完成（通过检查输出文件是否存在）
    :param module_name: 模块名称
    :param root_dir: 项目根目录
    :return: True表示输出文件已存在，可以跳过执行
    """
    output_files = {
        'm1_data_clean': os.path.join(root_dir, 'm1_data_clean', 'data', 'm1_final_clean.parquet'),
        'm2_stream_sim': os.path.join(root_dir, 'm2_stream_sim', 'model.pkl'),
        'm3_llm_feature': os.path.join(root_dir, 'm3_llm_feature', 'data', 'online_shopping_10_cats.csv'),
    }
    
    output_file = output_files.get(module_name)
    if output_file and os.path.exists(output_file):
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        print(f"{Color.WARNING}[WARN] 模块 [{module_name}] 输出文件已存在: {output_file}")
        print(f"{Color.WARNING}[WARN] 文件大小: {file_size:.2f} MB")
        return True
    return False


def run_full_pipeline(root_dir):
    """
    执行完整全链路流程：m1→m2→m3
    串行阻塞执行，任一模块失败则终止全链路
    支持跳过已完成的模块（输出文件已存在时）
    """
    print(f"\n{Color.INFO}[INFO] === 开始全链路执行 ===")
    print(f"{Color.INFO}[INFO] 执行顺序: m1_data_clean → m2_stream_sim → m3_llm_feature{Color.RESET}")
    print(f"{Color.INFO}[INFO] 检测已存在的输出文件，可跳过重复执行{Color.RESET}")
    
    # 定义模块配置：(模块名称, 目录名, 入口脚本)
    modules = [
        ('m1_data_clean', 'm1_data_clean', 'run_m1_pipeline.py'),
        ('m2_stream_sim', 'm2_stream_sim', 'train_model.py'),
        ('m3_llm_feature', 'm3_llm_feature', 'llm_batch_extract.py'),
    ]
    
    # 串行执行每个模块
    for module_name, module_dir, script_name in modules:
        # 检查是否可以跳过
        if check_module_output(module_name, root_dir):
            print(f"{Color.WARNING}[WARN] 跳过已完成的模块 [{module_name}]{Color.RESET}")
            continue
            
        success = run_module(module_name, module_dir, script_name, root_dir)
        if not success:
            print(f"{Color.ERROR}[ERROR] 模块 [{module_name}] 执行失败，终止全链路{Color.RESET}")
            # 清理已启动的子进程
            cleanup_all_processes()
            sys.exit(1)
    
    print(f"\n{Color.SUCCESS}[SUCCESS] === 全链路前置模块执行完成 ===")
    # 前置模块执行完毕，清理子进程
    cleanup_all_processes()


def cleanup_all_processes():
    """清理所有已启动的子进程"""
    global child_processes
    if child_processes:
        print(f"{Color.INFO}[INFO] 清理已启动的子进程...{Color.RESET}")
        for proc in child_processes:
            if proc.poll() is None:
                try:
                    kill_process_tree(proc.pid)
                except Exception as e:
                    print(f"{Color.WARNING}[WARN] 清理进程失败: {e}{Color.RESET}")
        child_processes = []


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='M5 项目一键启动脚本')
    parser.add_argument(
        '--dashboard-only',
        action='store_true',
        help='仅启动m4可视化看板，跳过m1/m2/m3前置模块（适用于前端调试）'
    )
    args = parser.parse_args()
    
    # 服务端口（统一使用8001）
    port = 8001
    root_dir = os.path.dirname(os.path.abspath(__file__))
    dashboard_dir = os.path.join(root_dir, 'm4_dashboard')
    
    print(f"{Color.INFO}[INFO] =======================================")
    print(f"{Color.INFO}[INFO]    M5 项目一键启动脚本")
    print(f"{Color.INFO}[INFO]    端口: {port}")
    if args.dashboard_only:
        print(f"{Color.INFO}[INFO]    模式: 快速调试看板模式")
    else:
        print(f"{Color.INFO}[INFO]    模式: 完整全链路模式")
    print(f"{Color.INFO}[INFO] ======================================={Color.RESET}")
    
    # 显示Windows环境警告
    if os.name == 'nt':
        print(f"{Color.WARNING}[WARN] Windows环境注意：已禁用--reload模式")
        print(f"{Color.WARNING}[WARN] 原因：--reload极易造成端口僵尸占用{Color.RESET}")
    
    # 环境自检
    if not env_check(root_dir):
        print(f"{Color.ERROR}[ERROR] 环境自检失败，退出程序{Color.RESET}")
        sys.exit(1)
    
    # 检查端口占用并尝试释放
    if not check_port(port):
        print(f"{Color.WARNING}[WARN] 端口 {port} 被占用，尝试自动释放...{Color.RESET}")
        if not release_port(port):
            print(f"{Color.ERROR}[ERROR] 无法释放端口 {port}，请手动释放后重试{Color.RESET}")
            print(f"{Color.WARNING}[WARN] 手动释放命令: taskkill /F /T /PID <PID>{Color.RESET}")
            sys.exit(1)
    print(f"{Color.SUCCESS}[SUCCESS] 端口 {port} 可用{Color.RESET}")
    
    # 【全链路模式】执行前置模块 m1→m2→m3
    if not args.dashboard_only:
        run_full_pipeline(root_dir)
    else:
        print(f"{Color.INFO}[INFO] 快速调试模式：跳过m1/m2/m3前置模块{Color.RESET}")
    
    # 构建启动命令（移除--reload，单进程模式）
    cmd = ['uvicorn', 'server:app', '--host', '0.0.0.0', '--port', str(port)]
    
    print(f"{Color.INFO}[INFO] 启动命令: {' '.join(cmd)}")
    print(f"{Color.INFO}[INFO] 工作目录: {dashboard_dir}{Color.RESET}")
    
    # 启动服务
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=dashboard_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
        )
        main_pid = proc.pid
        print(f"{Color.SUCCESS}[SUCCESS] FastAPI服务已启动，PID: {main_pid}{Color.RESET}")
        
    except Exception as e:
        print(f"{Color.ERROR}[ERROR] 启动失败: {e}{Color.RESET}")
        sys.exit(1)
    
    # 等待服务启动（轮询检测端口）
    print(f"{Color.INFO}[INFO] 等待服务启动...{Color.RESET}")
    max_retries = 60
    startup_success = False
    
    for i in range(max_retries):
        # 端口被占用说明服务已启动监听
        if not check_port(port):
            url = f'http://127.0.0.1:{port}'
            print(f"{Color.SUCCESS}[SUCCESS] 服务就绪: {url}{Color.RESET}")
            # 自动打开浏览器
            print(f"{Color.INFO}[INFO] 正在打开浏览器...{Color.RESET}")
            webbrowser.open(url)
            startup_success = True
            break
        time.sleep(1)
        if i % 10 == 0:
            print(f"{Color.INFO}[INFO] 等待中 ({i+1}/{max_retries})...{Color.RESET}")
    
    if not startup_success:
        print(f"{Color.ERROR}[ERROR] 服务启动超时{Color.RESET}")
        kill_process_tree(main_pid)
        sys.exit(1)
    
    # 服务运行中，等待中断信号
    print(f"{Color.INFO}[INFO] === 服务运行中 ===")
    print(f"{Color.WARNING}[WARN] 按 Ctrl+C 停止服务{Color.RESET}")
    
    def cleanup(signum=None, frame=None):
        """优雅清理函数：终止进程并释放端口"""
        print(f"\n{Color.WARNING}[WARN] 收到中断信号，正在停止服务...{Color.RESET}")
        
        # 终止主进程树
        kill_process_tree(main_pid)
        
        # 等待1秒后检查端口是否释放
        time.sleep(1)
        
        # 如果端口仍被占用，强制释放
        if not check_port(port):
            print(f"{Color.WARNING}[WARN] 端口 {port} 仍被占用，强制释放...{Color.RESET}")
            release_port(port)
        
        print(f"{Color.SUCCESS}[SUCCESS] 所有服务已停止{Color.RESET}")
        sys.exit(0)
    
    # 注册Ctrl+C信号处理
    signal.signal(signal.SIGINT, cleanup)
    
    # 主循环：保持进程运行
    try:
        while True:
            time.sleep(1)
            # 检查子进程是否异常退出
            if proc.poll() is not None:
                print(f"{Color.ERROR}[ERROR] 服务进程异常退出{Color.RESET}")
                cleanup()
    except KeyboardInterrupt:
        # 捕获Ctrl+C
        cleanup()


if __name__ == "__main__":
    """脚本入口"""
    main()