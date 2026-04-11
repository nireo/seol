module github.com/nireo/seol/benchmarks/dbcompare

go 1.26.1

require (
	github.com/nireo/seol v0.0.0
	github.com/syndtr/goleveldb v1.0.0
)

require (
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/nireo/seol => ../..
