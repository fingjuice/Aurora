#!/bin/bash

# ===============================
# 配置区
# ===============================
# GitHub 仓库 HTTPS 地址
GITHUB_URL="https://github.com/fingjuice/Aurora.git"

# Git 用户信息
GIT_USER_NAME="Aurora"
GIT_USER_EMAIL="1609638989@qq.com"

# ===============================
# 1. 初始化 Git 仓库（如果没有）
# ===============================
if [ ! -d ".git" ]; then
    echo "初始化本地 Git 仓库..."
    git init
else
    echo "本地 Git 仓库已存在"
fi

# ===============================
# 2. 设置全局用户名和邮箱
# ===============================
git config --global user.name "$GIT_USER_NAME"
git config --global user.email "$GIT_USER_EMAIL"

# ===============================
# 3. 添加远程仓库
# ===============================
if git remote | grep origin > /dev/null; then
    echo "远程 origin 已存在，更新 URL..."
    git remote set-url origin $GITHUB_URL
else
    echo "添加远程 origin..."
    git remote add origin $GITHUB_URL
fi

# ===============================
# 4. 添加所有文件并提交
# ===============================
git add .
git commit -m "Initial commit"

# ===============================
# 5. 设置默认分支为 main
# ===============================
git branch -M main

# ===============================
# 6. 推送到 GitHub（HTTPS）
# ===============================
echo "推送到 GitHub..."
git push -u origin main

# ===============================
# 7. 可选：生成 SSH Key（以后可用）
# ===============================
read -p "是否需要生成 SSH Key 用于 GitHub 连接？(y/n): " gen_ssh
if [ "$gen_ssh" == "y" ]; then
    ssh-keygen -t ed25519 -C "$GIT_USER_EMAIL"
    echo "公钥内容如下，复制到 GitHub SSH Keys："
    cat ~/.ssh/id_ed25519.pub
fi

echo "完成 ✅"

