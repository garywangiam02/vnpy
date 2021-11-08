echo "开始"
sync;
sync;
sync #写入硬盘，防止数据丢失
sleep 10 #延迟10秒
echo 1 > /proc/sys/vm/drop_caches
echo 2 > /proc/sys/vm/drop_caches
echo 3 > /proc/sys/vm/drop_caches
echo "结束"
