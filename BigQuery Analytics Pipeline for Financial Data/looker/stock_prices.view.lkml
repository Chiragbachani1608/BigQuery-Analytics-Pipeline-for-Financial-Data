view: stock_prices {
  sql_table_name: `financial-analytics-demo.financial_data.stock_prices` ;;
  
  dimension: date {
    type: date
    primary_key: yes
    sql: ${TABLE}.date ;;
  }
  
  dimension: symbol {
    type: string
    sql: ${TABLE}.symbol ;;
  }
  
  dimension: price_range {
    type: number
    sql: ${TABLE}.high - ${TABLE}.low ;;
  }
  
  measure: count {
    type: count
  }
  
  measure: average_close {
    type: average
    sql: ${TABLE}.close ;;
  }
  
  measure: total_volume {
    type: sum
    sql: ${TABLE}.volume ;;
  }
}
