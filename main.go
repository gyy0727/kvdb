package main

import "fmt"

const PORT = 8080

func main() {
	i := 0
	arr := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	brr :=arr[:0]
	for{
		if i == 10 {
			break
		}
		fmt.Println("arr = %d",arr[i]);
		i++;
	}
	fmt.Println("arr 的长度 : %d", len(arr))
	fmt.Println("brr 的长度 : %d", len(brr))
}
