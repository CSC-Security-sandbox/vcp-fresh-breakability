package auth

type JwtPayload struct {
	Subject    string `json:"sub"`
	Issuer     string `json:"iss"`
	Audience   string `json:"aud"`
	Expiration int64  `json:"exp"`
	IssuedAt   int64  `json:"iat"`
	Google     Google `json:"Google"`
}

type Google struct {
	ProjectNumber int64 `json:"project_number"`
}
