<?php

namespace itd;

if (!defined('IS_WIN')) {
    define('IS_WIN', strstr(PHP_OS, 'WIN') ? 1 : 0);
}

/**
 * Class NDBI 基于mysqli的数据库操作对象
 * @author lizr
 */
class NDBI
{
    /**
     * @var \mysqli php内置类
     */
    private $link;
    private $sql;

    private $updateTableMap = []; // 用于批量更新的单元 {{"id"=>{字段1=>值1,字段2=>值2,..},"id2"=>{字段1=>值3,字段2=>值4,..}.. }
    private $updatePriField = []; // 每个表批量更新的主键字段 {表名1 => 主键1,}
    private $flush_max = 500; // 达多少条记录时自动冲刷
    private $manyrows = 2000; // 扩展插入的最大条数 defaut 2000

    // 事务指令数
    protected $transTimes = 0;

    public static function getDBI($db_config)
    {
        static $_instance = array();
        $guid = md5(serialize($db_config));
        if (!isset ($_instance [$guid])) {
            $obj = new self($db_config);
            $_instance [$guid] = $obj;
        }
        return $_instance [$guid];
    }


    public function __construct($db_config)
    {
        // echo "NDBI.__construct:".json_encode($db_config);
        $port = $db_config[4] ?? 3306;
        try {
            $this->link = new \mysqli($db_config[0], $db_config[1], $db_config[2], $db_config[3], $port);
            $this->link->query("SET time_zone = '-4:00'");
            $this->link->query("SET NAMES UTF8");
        } catch (\Throwable $e) {
            print('mysqli connect error:' . $e->getCode() . ' ' . $e->getMessage());
        }
    }

    /**
     * 执行查询
     * @return mysqli_result | bool | int
     */
    public function query($sql)
    {
        $this->sql = $sql;
        $result = $this->link->query($sql) or $this->sql_error();
        if ($this->startwith($sql, ['DELETE', 'UPDATE'], false)) {
            return $this->link->affected_rows;
        } else if ($this->startwith($sql, ['INSERT'], false)) {
            return $this->link->insert_id;
        } else {
            if ($result === false && IS_WIN) {
                $this->dieTrackBack($sql);
            }
            return $result;
        }
    }

    // 插入
    function insert($table, $arr, $replace = false)
    {
        $fields = $values = '';
        foreach ($arr as $k => $v) {
            $fields .= "`" . $k . "`,";
            if ('NOW()' == $v) { //TODO 跟DBO
                $values .= $v . ",";
            } else
                $values .= "'" . str_replace("'", "''", $v) . "',";
        }
        $action = $replace ? 'REPLACE' : 'INSERT';
        $sql = $action . " INTO " . $table . " (" . trim($fields, ',') . ") VALUES (" . trim($values, ',') . ")";
        return $this->query($sql);
    }

    // 插入多行 有可能会报Incorrect integer value错误 去掉my.ini 中 sql-mode=STRICT_TRANS_TABLES
    function insertmany($table, $rows)
    {
        if ($rows) {
            $headsql = "INSERT INTO $table (" . implode(",", array_keys($rows [0])) . ") VALUES ";
            foreach (array_chunk($rows, $this->manyrows, true) as $rows_chunk) {
                $sql = $headsql;
                foreach ($rows_chunk as $row) {
                    $sql .= "('" . implode("','", array_values($row)) . "'),";
                }
                $sql = substr($sql, 0, -1);
                $this->query($sql);
            }
        }
    }


    // 更新 return 影响行数 TODO
    function update($table, $arr, $query = '', $is_origin = false)
    {
        $sql = "UPDATE $table SET ";
        // print_r($arr);
        $update_str = '';
        foreach ($arr as $k => $v) {
            if ($is_origin) {
                $value = "'" . $v . "'";
            } else {
                $value = "'" . str_replace("'", "\\'", $v) . "'"; // 有空再研究' 号的问题 不同于insert str_replace("'","''",$v)
            }
            $update_str .= "`" . $k . '`=' . $value . ',';
        }
        $sql .= trim($update_str, ',') . " WHERE " . $query;
        return $this->query($sql);
    }

    // 删除
    function delete($table, $query = '0')
    {
        $sql = "DELETE FROM $table WHERE $query";
        return $this->query($sql);
    }

    /**
     * 查出一个字段
     */
    public function out_field($table, $field, $query)
    {
        $sql = "SELECT $field FROM $table WHERE $query ";
        $result = $this->query($sql);
        $field_result = false;
        if ($result) {
            $row = $result->fetch_array();
            $field_result = isset($row[0]) ? $row[0] : false;
        }
        return $field_result;
    }

    // 返回多行中某外值组成的数组
    function out_list($table, $field, $query = '1', $distinct = true)
    {
        $arr = [];
        $sfield = $distinct ? "DISTINCT($field) AS $field" : $field;
        $sql = "SELECT $sfield FROM $table WHERE $query ";
        $result = $this->query($sql);
        if ($result) {
            while ($row = $result->fetch_array())
                $arr [] = $row [$field];
        }
        return $arr;
    }

    // 返回一行中的多个字段s
    function out_fields($table, $fields, $query = '1')
    {
        $sql = "SELECT $fields FROM $table WHERE $query ";
        $result = $this->query($sql);
        $row = $result->fetch_assoc();
        return $row;
    }

    // 返回一行
    function out_row($table, $query = '1')
    {
        return $this->out_fields($table, '*', $query);
    }

    // 返回多行
    function out_rows($table, $query = '1', $fields = '*')
    {
        $info = array();
        $sql = "SELECT $fields FROM $table WHERE $query ";
        $result = $this->query($sql);
        if ($result) {
            while ($row = $result->fetch_assoc()) {
                $info [] = $row;
            }
        }
        return $info;
    }

    //返回一行
    function out_row_sql($sql)
    {
        $result = $this->query($sql);
        $row = $result->fetch_assoc();
        return $row;
    }

    /**
     * windows状态下调试
     * @param $sql
     */
    private function dieTrackBack($sql)
    {
        if (IS_WIN) {
            echo "error sql:" . $sql . "<br>";
            echo "<pre>";
            debug_print_backtrace();
            echo "</pre>";
            die('die in NDBI.showTrackBack()');
        }
    }

    //返回多行
    public function out_rows_sql($sql, $key = '')
    {
        $info = array();
        if ($sql) {
            $result = $this->query($sql);
            while ($row = $result->fetch_assoc()) {
                if ($key !== "") {
                    $info [$row[$key]] = $row;
                } else {
                    $info [] = $row;
                }
            }
        }
        return $info;
    }

    // sql错误处理
    private function sql_error()
    {
        print ('<font color=red>' . mysqli_error($this->link) . '</font><hr><font color=red>' . $this->sql . '</font>');
    }

    // 返回多行,以个字段为key,请确保字段的值唯一
    public function out_rows_map($table, $field, $query = '1', $select_field = '*')
    {
        $info = array();
        $sql = "SELECT $select_field FROM $table WHERE $query ";
        $result = $this->query($sql);
        while ($row = $result->fetch_assoc()) {
            $info [$row [$field]] = $row;
        }
        return $info;
    }

    /**
     * 增加更新单元(暂只支持主键)
     * @param $table
     * @param string $pri_field 主键字段名
     * @param int $id 主键值
     * @param $cell
     */
    public function add_update_queue($table, $pri_field, $id, $cell)
    {
        if (isset ($this->updatePriField[$table]) && $this->updatePriField[$table] != $pri_field) {
            die ("die >>> db.add_update_queue 同一个表同一时间只能存在一个主键");
        }
        $this->updatePriField [$table] = $pri_field;

        if (!isset ($this->updateTableMap [$table])) {
            $this->updateTableMap [$table] = array();
        }
        $this->updateTableMap [$table] [$id] = $cell;
        // 达一定数量自动冲刷
        if (count($this->updateTableMap [$table]) >= $this->flush_max) {
            $this->flush_update_queue($table);
        }
    }

    /**
     * 冲刷准备批量更新的信息
     * @param null $table
     */
    public function flush_update_queue($table = null)
    {
        if ($table) {
            if (!empty ($this->updateTableMap [$table])) {
                $this->updatemany($table, $this->updatePriField [$table], $this->updateTableMap [$table]);
                $this->updateTableMap [$table] = array();
            }
        } else {
            foreach ($this->updateTableMap as $table => $rows) {
                $this->updatemany($table, $this->updatePriField [$table], $this->updateTableMap [$table]);
                $this->updateTableMap [$table] = array();
            }
        }
    }

    /**
     * 批量更新多条记录
     *
     * @param [] $rows 结构为
     *            {主键=>{字段1=>值1,字段2=>值2},...} --- 注意:如果值由 += 或 -= 开头 变为字段增加
     * @example UPDATE mytable SET
     *          field1 = CASE id
     *          WHEN 1 THEN 'value'
     *          WHEN 2 THEN 'value'
     *          WHEN 3 THEN 'value'
     *          END,
     *          field2 = CASE id
     *          WHEN 1 THEN 'value'
     *          WHEN 2 THEN 'value'
     *          WHEN 3 THEN 'value'
     *          END
     *          WHERE id IN (1,2,3)
     */
    public function updatemany($table, $priField, $rows)
    {
        $count = count($rows);
        if ($count > 0) {
            $ADD_SIGN = "+=";
            $MINUS_SIGN = "-=";
            $priValues = array_keys($rows);
            $where = $priField . " IN (" . implode(",", $priValues) . ")";
            $sql = "UPDATE $table SET ";
            $arrField = array_keys(array_values($rows) [0]); // 拿第一个数组的结构(因为每个数组的结构都一样)
            $str = "";
            foreach ($arrField as $field) {
                $str .= $field . " = CASE " . $priField;
                foreach ($priValues as $priValue) {
                    $value = $rows [$priValue] [$field];
                    if (strpos($value, $ADD_SIGN) === 0) {
                        $value = str_replace($ADD_SIGN, '', $value);
                        $str .= " WHEN " . $priValue . " THEN $field + '" . $value . "'";
                    } elseif (strpos($value, $MINUS_SIGN) === 0) {
                        $value = str_replace($MINUS_SIGN, '', $value);
                        $str .= " WHEN " . $priValue . " THEN $field - '" . $value . "'";
                    } else {
                        $str .= " WHEN " . $priValue . " THEN '" . $value . "'";
                    }
                }
                $str .= " END,";
            }
            $sql .= substr($str, 0, -1) . " WHERE " . $where;
            echo 'updatemany:' . $sql . '<br>';
            $this->query($sql);
        }
    }

    /**
     * 显示最后一句运行的sql
     */
    public function showLastSql()
    {
        echo $this->sql;
    }

    public function getSql()
    {
        return $this->sql;
    }

    /**
     * 选择数据库
     * @param $db
     */
    public function selectDb($db)
    {
        return mysqli_select_db($this->link, $db);
    }

    /**
     * 获取某个表的所有字段
     * @param $table
     * @return array
     */
    public function getDbFields($table)
    {
        $sql = "DESCRIBE $table";
        $result = $this->query($sql);
        $info = [];
        if ($result) {
            while ($row = $result->fetch_assoc()) {
                $info [] = $row['Field'];
            }
        }
        return $info;
    }

    /**
     * 检查是否$needle开头
     * @param string|array $needle
     * @return bool
     */
    private function startwith($str, $needle, $case_sensitive = true)
    {
        $func = $case_sensitive ? "strpos" : "stripos";
        if (is_array($needle)) {
            $flag = false;
            foreach ($needle as $item) {
                $flag = $flag || $func($str, $item) === 0;
                if ($flag) {
                    break;
                }
            }
            return $flag;
        } else {
            return $func($str, $needle) === 0;
        }
    }

    #---------------- 事务处理 ------------------
    public function startTrans()
    {
        if ($this->transTimes == 0) {
            $this->query('START TRANSACTION');
        }
        $this->transTimes++;
    }

    public function commit()
    {
        if ($this->transTimes > 0) {
            $this->query('COMMIT');
            $this->transTimes = 0;
        }
    }

    public function rollback()
    {
        if ($this->transTimes > 0) {
            $this->query('ROLLBACK');
            $this->transTimes = 0;
        }
    }
    #---------------END 事务处理 ------------------


}
