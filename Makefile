.PHONY: all clean

all:
	cargo build
	cp target/debug/3700kvstore ./

clean:
	cargo clean
	rm 3700kvstore