package args_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thrawn01/args"
)

var _ = Describe("KeyValueTokenizer", func() {

	Describe("Next()", func() {
		Context("Given a single key=value pair", func() {
			It("Should return the next token found in the buffer", func() {
				tokenizer := args.NewKeyValueTokenizer("key=value")

				Expect(tokenizer.Next()).To(Equal("key"))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value"))
				Expect(tokenizer.Next()).To(Equal(""))
				// Should not error
				Expect(tokenizer.Next()).To(Equal(""))
			})
		})
		Context("Given multiple key=value pairs", func() {
			It("Should return the next token found in the buffer", func() {
				tokenizer := args.NewKeyValueTokenizer("key=value,key2=value2")

				Expect(tokenizer.Next()).To(Equal("key"))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value"))
				Expect(tokenizer.Next()).To(Equal(","))
				Expect(tokenizer.Next()).To(Equal("key2"))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value2"))
				Expect(tokenizer.Next()).To(Equal(""))
				// Should not error
				Expect(tokenizer.Next()).To(Equal(""))
			})
		})
		Context("Given multiple key=value pairs with prefix and suffic space", func() {
			It("Should return the next token found in the buffer without the spaces", func() {
				tokenizer := args.NewKeyValueTokenizer("key =value , key2= value2")

				Expect(tokenizer.Next()).To(Equal("key"))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value"))
				Expect(tokenizer.Next()).To(Equal(","))
				Expect(tokenizer.Next()).To(Equal("key2"))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value2"))
				Expect(tokenizer.Next()).To(Equal(""))
				// Should not error
				Expect(tokenizer.Next()).To(Equal(""))
			})
		})
		Context(`Given an escaped delimiter key\==value`, func() {
			It("Should respect the escaped delimiter", func() {
				tokenizer := args.NewKeyValueTokenizer(`http\=ip=value\=`)

				Expect(tokenizer.Next()).To(Equal(`http=ip`))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value="))
				Expect(tokenizer.Next()).To(Equal(""))
				// Should not error
				Expect(tokenizer.Next()).To(Equal(""))

				tokenizer = args.NewKeyValueTokenizer(`key\,=value\=`)

				Expect(tokenizer.Next()).To(Equal(`key,`))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value="))
				Expect(tokenizer.Next()).To(Equal(""))
				// Should not error
				Expect(tokenizer.Next()).To(Equal(""))
			})
		})
		Context(`Given an escaped escaped delimiter key\\=value`, func() {
			It("Should respect the escaped escaped delimiter", func() {
				tokenizer := args.NewKeyValueTokenizer(`key\\=value`)

				Expect(tokenizer.Next()).To(Equal(`key\\`))
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal("value"))
				Expect(tokenizer.Next()).To(Equal(""))
				// Should not error
				Expect(tokenizer.Next()).To(Equal(""))
			})
		})
		Context("Given malformed buffer", func() {
			It("Should return a valid token", func() {
				tokenizer := args.NewKeyValueTokenizer("value")
				Expect(tokenizer.Next()).To(Equal("value"))
				Expect(tokenizer.Next()).To(Equal(""))

				tokenizer = args.NewKeyValueTokenizer(",")
				Expect(tokenizer.Next()).To(Equal(","))
				Expect(tokenizer.Next()).To(Equal(""))

				tokenizer = args.NewKeyValueTokenizer("=")
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal(""))

				tokenizer = args.NewKeyValueTokenizer("=,")
				Expect(tokenizer.Next()).To(Equal("="))
				Expect(tokenizer.Next()).To(Equal(","))
				Expect(tokenizer.Next()).To(Equal(""))

				tokenizer = args.NewKeyValueTokenizer(`{"blue":"bell"}`)
				Expect(tokenizer.Next()).To(Equal(`{"blue":"bell"}`))
				Expect(tokenizer.Next()).To(Equal(""))
			})
		})
	})
})
